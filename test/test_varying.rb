require 'active_support'

module TestVarying
  extend ActiveSupport::Concern

  def test_varying_logger
    if defined?(logger)
      logger
    elsif defined?(Rails)
      Rails.logger
    else
      Logger.new($stdout)
    end
  end

  module ClassMethods

    # The following will generate 4 tests
    #
    #   test_varying 'my test',
    #                mybool: true,
    #                somevar: [:a, :b] do |variant|
    #     ...
    #     if variant.mybool  # will get true and false
    #       ...
    #       variant.somevar  # will get :a and :b
    #       ....
    #     end
    #   end
    #
    def test_varying name, variations={}, &block
      raise ArgumentError unless variations.count >= 1
      keys = variations.keys
      vals = keys.map{|key| variations[key] }
      vals = normalize_to_arrays vals

      variant_klass = Struct.new(*keys)
      vals[0].product(*vals[1..-1]).each do |varieds|
        vsummary = varieds.map.with_index{|v, i| "#{keys[i]}=#{v.inspect}" }.join('|')
        test "#{name} #{vsummary}" do
          test_varying_logger.info "---- Running test #{name} #{varieds.join('|')}"
          instance_exec(variant_klass.new(*varieds), &block)
        end
      end
    end


    # ---- Private utilities
    #
    private
    def normalize_to_arrays outer
      outer.map{|i|
        case i
          when TrueClass
            [true, false]
          when Range, Enumerable
            i.to_a
          else
            raise ArgumentError, "cannot use type #{i.inspect}"
        end
      }
    end

  end


end
