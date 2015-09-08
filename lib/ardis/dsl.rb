module Ardis
module DSL

  # Usage:
  #
  # series_list       :friend_feed, global: true, relation: Collage, key: lambda{|class| }
  # series_list       :friend_feed,     relation: Collage, key: lambda{|user| }
  # series_sorted_set :liked_collages,  relation: Collage, key: lambda{|user| }, attr_score: :created_at
  # series_set        :friend_feed,     relation: Collage, key: lambda{|user| }

  module BaseDSL
    def define_series(series_class: raise(ArgumentError),
                      name:         raise(ArgumentError),
                      global:       false,
                      **series_opts,
                      &extension_block)
      klass =  if global
                  singleton_class
                else
                  self
                end

      klass.send :define_method, name do                  
        iname = "@#{name}"
        instance_variable_get(iname) or begin
          if global
            series_opts[:relation] ||= self  # Global and no relation given, default to class
          end
          series = series_class.new(name: name, container: self, **series_opts, &extension_block)
          instance_variable_set(iname, series)
        end
      end

      # Method for accessing the Series but not having to instantitate the
      # origin object.
      #
      if !global
        metaclass = (class << klass; self; end)
        metaclass.send :define_method, "series_for_#{name}" do |opts={}|
          series_class.new(name: name, container: opts[:container], **series_opts)
        end
      end

    end
  end

  module ListDSL
    include BaseDSL
    def series_list **opts, &block
      define_series series_class: Ardis::RedisAdapter::ListSeries, **opts, &block
    end
  end

  module SortedSetDSL
    include BaseDSL
    def series_sorted_set **opts, &block
      define_series series_class: Ardis::RedisAdapter::SortedSetSeries, **opts, &block
    end
  end

  module SetDSL
    include BaseDSL
    def series_set **opts, &block
      define_series series_class: Ardis::RedisAdapter::SetSeries, **opts, &block
    end
  end

  module ReferenceDSL
    include BaseDSL
    def series_reference association, series_name

      self.send :define_method, "#{association}_#{series_name}" do
        assoc_reflection = self.class.reflect_on_association(association)
        assoc_id    = send(assoc_reflection.foreign_key)
        assoc_class = assoc_reflection.klass
        container_proxy = Ardis::ContainerProxy.new(assoc_class, assoc_id)
        assoc_class.send "series_for_#{series_name}", container: container_proxy
      end

    end
  end

end
end
