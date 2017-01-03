# Include this way, otherwise will get a warning about the absence of a Framework,
# see https://github.com/amatsuda/kaminari/issues/518.
#
require 'kaminari/config'
require 'kaminari/helpers/action_view_extension'
require 'kaminari/helpers/paginator'
require 'kaminari/models/page_scope_methods'
require 'kaminari/models/configuration_methods'
require 'kaminari/hooks'

require_relative 'autocompacter'

module Ardis

  class BaseSeries
    include Enumerable

    attr_reader   :name,
                  :container,
                  :relation,
                  :initializer,
                  :inverse_of,
                  :decorator

    # PRIVATE accessors
    attr_accessor :where_values,
                  :limit_value,
                  :offset_value,
                  :order_values,
                  :reverse_order_value,
                  :readonly_value,
                  :includes_values,
                  :select_values,
                  # -------------------------------------------
                  :autocompact_value

    # ---------------------------------------------------------
    # Object lifecycle
    def initialize( name:             nil,
                    relation:         ,
                    container:        nil,
                    initializer:      nil,
                    inverse_of:       nil,
                    decorator:        nil,
                    expiration:       nil,
                    &extension_block)

      @name             = name
      @container        = container
      @relation         = relation
      @initializer      = initializer
      @inverse_of       = inverse_of
      @decorator        = decorator
      @expiration       = expiration

      # ActiveRecord relation bookkeeping attributes
      @where_values     = []
      @includes_values  = []
      @select_values    = []

      # Extension block
      if extension_block
        instance_eval &extension_block
      end

    end

    # --------------------------------------------------------
    # Public Series interface

    public
    def ids
      resolve_ids
    end
    def total_count
      raise NotImplementedError, 'subclass must implement'
    end
    def calculate_count
      resolve_relation.where(id: resolve_ids).count
    end
    def del
      raise NotImplementedError, 'subclass must implement'
    end
    def exists?
      raise NotImplementedError, 'subclass must implement'
    end

    # ---------------------------------------------------------
    # ActiveRecord::Relation-mimic interface

    public
    def limit value
      series = clone
      series.limit_value = value
      series
    end
    def limit?(value)
      limit_value ? self : limit(value)
    end
    def offset value
      series = clone
      series.offset_value = value
      series
    end
    def offset?(value)
      offset_value ? self : offset(value)
    end
    def reverse_order value=!reverse_order_value
      series = clone
      series.reverse_order_value = value
      series
    end
    def readonly value=true
      series = clone
      series.readonly_value = value
      series
    end
    def includes *args
      args.reject!{ |a| a.blank? }
      return self if args.blank?

      series = clone
      series.includes_values = (series.includes_values + args).flatten.uniq
      series
    end
    def select *args
      args.reject!{ |a| a.blank? }
      return self if args.blank?

      series = clone
      series.select_values = (series.select_values + args).flatten.uniq
      series
    end
    def where *args
      series = clone
      series.where_values = series.where_values + [args]
      series
    end
    def includes_if bool, *args
      bool ? includes(*args) : self
    end
    def autocompact
      series = clone
      series.autocompact_value = !series.autocompact_value
      series
    end

    # ------------------------------------------------------------
    # Kaminari-like functionality
    #

    # Add methods like `per`, `padding`, etc.
    include Kaminari::PageScopeMethods

    # These 3 methods are required by the Kaminari::PageScopeMethods methods, we
    # delegate them to the relation.
    # See kaminari/lib/kaminari/models/configuration_methods.rb
    #
    def default_per_page;     actual_relation.default_per_page; end
    def max_per_page;         actual_relation.max_per_page; end
    def max_pages;            actual_relation.max_pages; end

    # Define the `page` method. See
    # See kaminari/lib/kaminari/models/active_record_model_extension.rb
    #
    class_eval <<-RUBY, __FILE__, __LINE__ + 1
      def #{Kaminari.config.page_method_name}(num = 1)
        limit(default_per_page).offset(default_per_page * ([num.to_i, 1].max - 1))
      end
    RUBY

    # ---------------------------------------------------------
    # ActiveRecord::Relation resolving interface

    def to_a
      finished = false
      ret = Autocompacter.autocompact(offset_value || 0,
                                      limit_value || 1_000_000,
                                      deleting: autocompact_value) do |offset, limit|

        next nil if finished

        # Fetch. If we get less than we requested means there are no more,
        # so setup so next time we get called will return nil
        ids = fetch_ids(offset, limit, reverse: reverse_order_value)
        if ids.count < limit
          finished = true
        end

        # Return ids converted to objects
        next resolve_objs(ids, deleting: autocompact_value)
      end
    end


    def each &block # Required for Enumerable interface
      to_a.each &block
    end

    def map &block
      to_a.map &block
    end

    def first
      limit(1).to_a.first
    end
    def last
      reverse_order.first
    end

    def count
      [ [ total_count - (offset_value || 0),
          (limit_value || total_count) ].min,
        0 ].max
    end
    def empty?
      total_count == 0
    end
    def include? obj
      # TODO: Check if `actual_relation` was even given, deal with `actual_relation` nil elsewhere

      # Check is in relation, but only if we were given an object
      return false if obj.respond_to?(:id) && !(actual_relation.klass === obj)

      # Check to see if in DB. ActiveRecord `exists?` has to be given an id.
      return actual_relation.exists? ensure_id(obj)

      # Subclass should override and actually check that the id matches also.
    end

    def sample n=1
      raise 'autocompact not currently supported' if autocompact_value
      ids  = resolve_ids.sample(n)
      # Keep in mind instantiate_objs will compare objects against the relation
      # so if some of the objects found this way does not comply with the relation
      # any more you might get nils or an empty array back.
      #
      objs = instantiate_objs(ids).map(&:last).compact
      objs
    end

    # ---------------------------------------------------------
    # Public interface for insertion and deletion.
    # None of these should affect the state of the *Ruby* object, so no need to clone
    #
    public

    # `objs` put in can be ids or objects with respond to id
    # Options:
    # - :inversion_disabled
    #
    def push *objs
      options = (objs.pop if Hash === objs.last) || {}
      objs = objs.compact.flatten
      _insert objs, prepend: false, inversion_disabled: options[:inversion_disabled]
    end
    def << obj
      _insert [obj].compact.flatten
      self
    end
    def unshift *objs
      options = (objs.pop if Hash === objs.last) || {}
      objs = objs.compact.flatten
      _insert objs, prepend: true, inversion_disabled: options[:inversion_disabled]
    end

    # `objs` put in can be ids or objects with respond to id.
    # Will try return the # of objects deleted (though might not work for some
    # implementations, for example Redis if pipelining)
    # Options:
    # - :inversion_disabled
    #
    def delete *objs
      options = (objs.pop if Hash === objs.last) || {}
      objs = objs.compact.flatten
      _delete objs, inversion_disabled: options[:inversion_disabled]
    end
    def shift n=1
      # TODO: for now just do it generically with `delete`. Later use specific, like LPOP.
      if n == 1
        first.tap { |obj|
          delete obj if obj
        }
      else
        limit(n).to_a.tap { |objs|
          delete *objs if objs
        }
      end
    end

    protected
    def _insert objs, prepend: false, inversion_disabled: false
      ids = objs.map{|o| ensure_id(o) }

      insert_count = insert_ids ids, prepend: prepend

      # Update and invert
      if insert_count > 0
        update_updated_at updated_count: insert_count
        _insert_inverse(objs, prepend: prepend) unless inversion_disabled
      end

      insert_count
    end
    def _insert_inverse objs, prepend: false
      if inverse_of
        raise ArgumentError if not container
        objs.each do |obj|
          if inv = obj.try(inverse_of)
            inv.send :_insert, [container], prepend: prepend, inversion_disabled: true
          end
        end
      end
    end

    # Should return the # of items deleted (though it might not be possible in some
    # implementations).
    #
    def _delete objs, inversion_disabled: false
      ids = objs.map{|o| ensure_id(o) }

      # Delete from underlying store
      delete_count = delete_ids(ids)

      # We can't check for *which* deleted_ids here which is actually better,
      # because in some cases (e.g. Redis pipelined requests) they are not available.
      if delete_count.present?
        update_updated_at updated_count: delete_count
        _delete_inverse(objs) unless inversion_disabled
      end

      delete_count
    end
    def _delete_inverse objs
      if inverse_of
        raise ArgumentError if not container
        objs.each do |obj|
          if inv = obj.try(inverse_of)
            inv._delete [container], inversion_disabled: true
          end
        end
      end
    end

    # ---------------------------------------------------------
    # Setters/getters

    # ---------------------------------------------------------
    # updated_at - subclasses should override

    public

    # Will return Time.at(0) if the Series key hasn't actually ever been initialized,
    # But will try to run the initializer if it hasn't been run.
    #
    def updated_at
      run_initializer?
      read_updated_at
    end
    def touch
      update_updated_at
    end

    protected
    def read_updated_at
      raise NotImplementedError, 'subclass must implement'
    end
    def update_updated_at updated_count: nil
      raise NotImplementedError, 'subclass must implement'
    end

    # --------------------------------------------------------
    # Internal utility

    protected
    def actual_relation
      r = relation
      if String === r || Symbol === r
        container.send relation
      elsif Proc === r
        relation.call container
      elsif ActiveRecord::Relation === r
        relation
      elsif r < ActiveRecord::Base
        relation.all
      else
        raise ArgumentError, "invalid relation value #{relation.inspect}"
      end
    end
    def ensure_id obj
      # TODO: check if obj already "id" (Integer or String), if not call method/lambda/send(:id)
      if obj
        obj.respond_to?(:id) ? obj.id : obj
      end
    end

    def resolve_objs ids, deleting: false
      id_objs = instantiate_objs(ids)
      if deleting
        if nils = id_objs.map{|id, obj| id if !obj }.compact.presence
          compact_ids nils
        end
        id_objs.map(&:last).compact
      else
        id_objs.map(&:last)
      end
    end

    def instantiate_objs ids

      # Retrieve objects from DB. TODO: Implement configurable "instantiator"
      objs = resolve_relation.where(id: ids)

      # Map by id
      obj_hash = Hash[objs.map{|obj| [obj.id, obj] }]

      # Output with ids
      ids.map{|id|
        obj = obj_hash[id.to_i]
        obj = decorator.new(obj) if decorator && obj
        [ id, obj ]
      }
    end

    def resolve_ids
      # Get IDs from Redis, possibly reversed
      ids = fetch_ids(offset_value || 0, limit_value, reverse: reverse_order_value)
      ids
    end
    def resolve_relation
      # Perform the actual chaining on an instance of ActiveRecord::Relation
      rel = actual_relation
      rel = rel.includes(*includes_values) unless includes_values.blank?
      rel = rel.select(*select_values) unless select_values.blank?
      rel = rel.reverse_order if reverse_order_value
      rel = rel.readonly(readonly_value) if readonly_value
      where_values.each{ |wv|
        rel = rel.where(*wv)
      }
      rel
    end

    # Returns true if there ids were deleted, otherwise false
    def compact_ids ids
      delete_count = delete_ids ids
      if delete_count > 0
        _compact_ids_inverse ids
        update_updated_at updated_count: delete_count
        true
      else
        false
      end
    end

    # For the inverse Series of each of the compacted objects, remove our container
    # from them.
    # Note that we don't handle the case in which the compacted id's object was already
    # removed from the database (i.e. the `where` below doesn't return it).
    # Presumably that objects' entire Series would be
    # destroyed along with it (e.g. in a after_destroy callback).
    #
    def _compact_ids_inverse ids
      if inverse_of
        actual_relation.model.where(id: ids).each { |obj|
          if inv_series = obj.try(inverse_of)
            inv_series.delete container
          end
        }
      end
    end

    # -----------------------------------------------------
    # Initializer
    #
    def run_initializer?
      # By default, do nothing
    end
    def call_initializer
      case initializer
        when Symbol, String
          container.send initializer
        else
          initializer.call container
      end
    end


    # ------------------------------------------------------
    # Required id resolution overrides

    # Must return the count of inserted objects
    def insert_ids ids, prepend: false
      raise NotImplementedError, 'subclass must implement'
    end

    def fetch_ids offset=0, limit=nil, reverse: false
      raise NotImplementedError, 'subclass must implement'
    end

    # Must return the count of deleted objects
    def delete_ids ids
      raise NotImplementedError, 'subclass must implement'
    end

  end

end