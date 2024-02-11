# CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
# CREATE TABLE jobs (
#   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
#   worker_name VARCHAR(255) NOT NULL,
#   status VARCHAR(50) NOT NULL DEFAULT 'queued',
#   payload JSONB NOT NULL,
#   created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
#   updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
# );

require "db"
require "json"
require "pg"

CONNECTION = DB.open("postgres://postgres@localhost:5432/elephante")

module Mosquito::Serializers::Primitives
  def serialize_string(str : String) : String
    str
  end

  def deserialize_string(raw : String) : String
    raw
  end

  def serialize_bool(value : Bool) : String
    value.to_s
  end

  def deserialize_bool(raw : String) : Bool
    raw == "true"
  end

  def serialize_symbol(sym : Symbol) : Nil
    raise "Symbols cannot be deserialized. Stringify your symbol first to pass it as a mosquito job parameter."
  end

  def serialize_char(char : Char) : String
    char.to_s
  end

  def deserialize_char(raw : String) : Char
    raw[0]
  end

  def serialize_uuid(uuid : UUID) : String
    uuid.to_s
  end

  def deserialize_uuid(raw : String) : UUID
    UUID.new raw
  end

  {% begin %}
    {%
      primitives = [
        {Int8, :to_i8},
        {Int16, :to_i16},
        {Int32, :to_i32},
        {Int64, :to_i64},
        {Int128, :to_i128},

        {UInt8, :to_u8},
        {UInt16, :to_u16},
        {UInt32, :to_u32},
        {UInt64, :to_u64},
        {UInt128, :to_u128},

        {Float32, :to_f32},
        {Float64, :to_f64},
      ]
    %}
     {% for mapping in primitives %}
        {%
          type = mapping.first
          method_suffix = type.stringify.underscore
          method = mapping.last
        %}

        def serialize_{{ method_suffix.id }}(value) : String
          value.to_s
        end

        def deserialize_{{ method_suffix.id }}(raw : String) : {{ type.id }}?
          if raw
            raw.{{ method.id }}
          end
        end
    {% end %}
  {% end %}
end

module Elephante
  class Base
    class_getter mapping = {} of String => Elephante::Job.class

    def self.register_job_mapping(string, klass)
      @@mapping[string] = klass
    end
  end

  abstract class Job
    include Mosquito::Serializers::Primitives

    macro inherited
      def self.worker_name
        "{{ @type.id }}".underscore.downcase
      end

      Elephante::Base.register_job_mapping(worker_name, {{ @type.id }})

      PARAMETERS = [] of Nil

      macro param(parameter)
        {% verbatim do %}
          {%
            a = "multiline macro hack"

            if !parameter.is_a?(TypeDeclaration) || parameter.type.nil? || parameter.type.is_a?(Generic) || parameter.type.is_a?(Union)
              message = <<-TEXT
              Mosquito::QueuedJob: Unable to build parameter serialization for `#{parameter.type}` in param declaration `#{parameter}`.

              Mosquito covers most of the crystal primitives for serialization out of the box[1]. More complex types
              either need to be serialized yourself (recommended) or implement custom serializer logic[2].

              Parameter types must be specified explicitly. Make sure your parameter declarations look something like this:

                class LongJob < Mosquito::QueuedJob
                  param user_email : String
                end

              Check the manual on declaring job parameters [3] if needed

              [1] - https://mosquito-cr.github.io/manual/index.html#primitive-serialization
              [2] - https://mosquito-cr.github.io/manual/serialization.html
              [3] - https://mosquito-cr.github.io/manual/index.html#parameters
              TEXT

              raise message
            end

            name = parameter.var
            value = parameter.value
            type = parameter.type
            simplified_type = type.resolve

            method_suffix = simplified_type.stringify.underscore.gsub(/::/, "__").id

            PARAMETERS << {
              name:          name,
              value:         value,
              type:          type,
              method_suffix: method_suffix,
            }
          %}

          @{{ name }} : {{ type }}?

          def {{ name }}=(value : {{simplified_type}}) : {{simplified_type}}
            @{{ name }} = value
          end

          def {{ name }}? : {{ simplified_type }} | Nil
            @{{ name }}
          end

          def {{ name }} : {{ simplified_type }}
            if ! (%object = {{ name }}?).nil?
                %object
            else
              msg = <<-MSG
                Expected a parameter named `{{ name }}` but found nil.
                The parameter may not have been provided when the job was enqueued.
                Should you be using `{{ name }}` instead?
              MSG
              raise msg
            end
          end
        {% end %}
      end

      macro finished
        {% verbatim do %}
          def initialize; end

          def initialize({{
                           PARAMETERS.map do |parameter|
                             assignment = "@#{parameter["name"]}"
                             assignment = assignment + " : #{parameter["type"]}" if parameter["type"]
                             assignment = assignment + " = #{parameter["value"]}" unless parameter["value"].is_a? Nop
                             assignment
                           end.join(", ").id
                         }})
          end

          # Methods declared in here have the side effect over overwriting any overrides which may have been implemented
          # otherwise in the job class. In order to allow folks to override the behavior here, these methods are only
          # injected if none already exists.

          {% unless @type.methods.map(&.name).includes?(:vars_from.id) %}
            def vars_from(config : Hash(String, String))
              {% for parameter in PARAMETERS %}
                @{{ parameter["name"] }} = deserialize_{{ parameter["method_suffix"] }}(config["{{ parameter["name"] }}"])
              {% end %}
            end
          {% end %}

          {% unless @type.methods.map(&.name).includes?(:build_job_run.id) %}
            def build_job_run
              data = {} of String => String
              {% for parameter in PARAMETERS %}
                data["{{ parameter["name"] }}"] = serialize_{{ parameter["method_suffix"] }}(@{{ parameter["name"] }}.not_nil!)
              {% end %}
              data
            end
          {% end %}

          def worker_name
            "{{ @type.id }}".underscore.downcase
          end
        {% end %}
      end

      def enqueue
        query = <<-EOF
        INSERT INTO jobs (
          worker_name,
          payload,
          created_at,
          updated_at
        ) VALUES (
          $1,
          $2,
          NOW(),
          NOW()
        )
        EOF

        CONNECTION.exec(query, worker_name, build_job_run.to_json)
      end
    end
  end

  class Worker
    def self.start
      puts "Starting Worker..."

      while true
        self.run
      end
    end

    def self.run
      job_id : UUID? = nil
      worker_name : String? = nil
      payload : JSON::Any? = nil

      CONNECTION.transaction do |tx|
        query = <<-EOF
          SELECT id, worker_name, payload
          FROM jobs
          WHERE status = 'queued'
          ORDER BY created_at ASC, id ASC
          LIMIT 1
          FOR UPDATE
        EOF

        job_id, worker_name, payload = tx.connection.query_one(query, as: {UUID, String, JSON::Any})
        tx.commit
      rescue DB::NoResultsError
        print '.'
        sleep 1
      end

      if payload
        begin
          klass = Elephante::Base.mapping[worker_name.as(String)]
          instance = klass.new

          adjusted_payload = {} of String => String
          payload.as_h.each do |key, value|
            adjusted_payload[key] = value.to_s
          end

          instance.vars_from(adjusted_payload)
          instance.perform

          CONNECTION.exec("UPDATE jobs SET status = 'completed', updated_at = NOW() WHERE id = $1", job_id)
        rescue e
          CONNECTION.exec("UPDATE jobs SET status = 'failed', updated_at = NOW() WHERE id = $1", job_id)
        end
      end
    end
  end
end

class LongJob < Elephante::Job
  param hard_job_data : String
  param foobar : String
  param count : Int32

  def perform
    puts "=" * 50
    pp! hard_job_data
    pp! foobar
    pp! count
    puts "=" * 50
  end
end

class LongerJob < Elephante::Job
  param hard_job_data : String

  def perform
    puts hard_job_data
  end
end

1_00.times do
  LongJob.new(hard_job_data: "this is long job", foobar: "barfoo", count: 1).enqueue
  LongerJob.new(hard_job_data: "this is longer job").enqueue
end

Elephante::Worker.start
