require 'draper'

module Ardis

class DynamoSeries < BaseSeries
  include AttrStrategy
  include Draper::Decoratable

  cattr_accessor :meta_table_name

  META_TABLE_ATTR_SERIES_NAME     = 'n'
  META_TABLE_ATTR_COUNT           = 'c'
  META_TABLE_ATTR_UPDATED_AT      = 't'


  attr_reader   :table_name,
                :local_key,
                :foreign_class,
                :foreign_key,
                :attributes,
                :default_index

  # PRIVATE attributes
  attr_accessor :index_by_value,
                :attributes_strategy

  def initialize( table_name:     nil,     # Can be implied from container and name
                  local_key:      nil,     # Can be implied from container
                  foreign_key:    nil,     # Can be implied from relation
                  attributes:     nil,     # Used to generate extra attributes
                  default_index:  nil,
                  memcache_count: false,
                  **options,
                  &extension_block)

    super options, &extension_block

    # For now, require container
    raise 'container required' unless self.container

    # Setup default properties
    @name           ||= relation.base_class.name.demodulize.underscore.pluralize
    @table_name     = table_name    || "#{container.class.name.demodulize}_#{name}".underscore
    @local_key      = local_key     || "#{container.class.name.demodulize}_id".underscore
    @foreign_key    = foreign_key   || "#{relation.base_class.name.demodulize}_id".underscore
    @default_index  = default_index
    @memcache_count = memcache_count

    # This should set the strategy
    self.attributes = attributes

  end

  # ---------------------------------------------------------
  # Setters/getters

  def attributes= val
    @attributes = val

    case val
      when nil
        self.attributes_strategy = nil
      when Symbol, String
        self.attributes_strategy = NamedAttrStrategy.new(val)
      when Proc
        self.attributes_strategy = ProcAttrStrategy.new(val)
      else
        raise ArgumentError, "invalid #{val.inspect}"
    end
  end

  # ---------------------------------------------------------
  # Additional ActiveRecord::Relation-mimic interface

  def index_by index
    series = clone
    series.index_by_value = index
    series
  end

  # ----------------------------------------------------------------------
  # Required Series overrides

  def total_count
    read_count
  end

  # -------
  def insert_ids ids, prepend: false
    insert_count = 0
    ids.each do |id|
      dmember_attributes = if @extracted_attributes && (a = @extracted_attributes[id.to_i])
                             dmembers_from_ruby(a)
                           end
      resp = ddb.put_item(
          table_name: table_name,
          return_values: 'ALL_OLD',
          item: {
              local_key         => dmember_from_ruby(container.id.to_i),
              foreign_key       => dmember_from_ruby(id.to_i)
          }
          .merge('t' => dmember_from_ruby(Time.now.to_f))
          .merge(dmember_attributes || {})
        )

      # If no previous attributes then it was a new record
      insert_count += 1 unless resp[:attributes]
    end
    update_count(insert_count) if insert_count > 0
    insert_count
  end

  def fetch_ids offset=0, limit=nil, reverse: false
    items = []


    # Calculate our DynamoDB query
    l               = offset + (limit or 1_000_000)
    index           = index_by_value || default_index
    key_conditions  = {
                        local_key => {
                          comparison_operator: 'EQ',
                          attribute_value_list: [ dmember_from_ruby(container.id.to_i) ],
                        }
                      }
    query           = {
                        table_name: table_name,
                        key_conditions: key_conditions,
                        limit: l,
                        scan_index_forward: !reverse_order_value
                      }.merge_if(index, index_name: index.to_s)

    # Check to see if we need to get more
    while 1

      # Do query
      resp    = ddb.query(query)

      # Process for next query
      items   += resp.member.to_a[offset..-1] || []
      resp_n  = resp.member.count
      l       = [0, l      - resp_n].max
      offset  = [0, offset - resp_n].max
      break if l <= 0 || resp[:last_evaluated_key].blank?

      # Update query
      query = query.merge(limit:                l,
                          exclusive_start_key:  resp[:last_evaluated_key])

    end

    # Go thru response, setting attributes
    @reportable_attributes = {}
    ids = items.map do |i|
      if v = i[foreign_key.to_s]
        id = dmember_to_ruby(v)
        if id
          id = id.to_i

          @reportable_attributes[id] = dmembers_to_ruby(i)

          id
        end
      end
    end

    ids
  end

  def delete_ids ids
    delete_count = 0
    ids.each do |id|
      resp = ddb.delete_item(
          table_name: table_name,
          return_values: 'ALL_OLD',
          key: {
              local_key   => dmember_from_ruby(container.id.to_i),
              foreign_key => dmember_from_ruby(id.to_i)
          }
      )
      delete_count += 1 if resp[:attributes]
    end
    update_count(-delete_count)
    delete_count
  end

  # --------

  # Params:
  #  - updated_count: # of items updated in a previous operation.
  #                   `nil` means we don't know.
  #                   Subclass implementers can deal with this information appropriately.
  #
  def update_updated_at updated_count: nil

    # If we know that we updated previously then no need to write again
    # the timestamp (because we updated it when we updated the count).
    #
    if updated_count.nil?
      ddb.update_item(
          table_name: self.class.meta_table_name,
          key: { META_TABLE_ATTR_SERIES_NAME => { 'S' => _series_name } },
          attribute_updates: {
              META_TABLE_ATTR_UPDATED_AT => { action: 'PUT',
                                              value: dmember_from_ruby(Time.now.to_f) }
          }
      )
    end
  end
  def read_updated_at
    resp = ddb.query(
        table_name: self.class.meta_table_name,
        attributes_to_get: [ META_TABLE_ATTR_UPDATED_AT ],
        key_conditions: {
            META_TABLE_ATTR_SERIES_NAME => {
                attribute_value_list: [{ 'S' => _series_name }],
                comparison_operator: 'EQ',
            }
        }
    )

    # If we know that we are > COUNT_THRESHOLD, but somehow end up <, then
    # just return COUNT_THRESHOLD + 1
    v = resp.member[0] && resp.member[0][META_TABLE_ATTR_UPDATED_AT]
    return v && Time.at(dmember_to_ruby(v).to_f)
  end

  # --------

  # `include_db`: if `true` will check that the object satisfies the relation
  # (i.e. is in the database and queriable via the relation). If `false` then only
  # check that the object's id is in the Series.
  #
  def include? obj, include_db: true
    raise 'offset and limit not supported with include? for DynamoSeries' \
      if offset_value || limit_value

    # See if we need to check the database object
    if include_db && !super(obj)
      return false
    end

    # Main handling, check to see if DynamoDB
    id = ensure_id(obj)
    resp = ddb.get_item(
        table_name: table_name,
        key: {
            local_key   => dmember_from_ruby(container.id.to_i),
            foreign_key => dmember_from_ruby(id.to_i)
        }
    )
    !resp.empty?
  end

  # --------
  def del
    # Loop thru query + delete
    while 1
      ids = fetch_ids(0, 1000)
      break if ids.blank?
      ids.each_slice(25) do |ids|
        ddb.batch_write_item(
          request_items: {
              table_name => ids.map{|id|
                {
                    'DeleteRequest' => {
                        'Key' => {
                            local_key   => dmember_from_ruby(container.id.to_i),
                            foreign_key => dmember_from_ruby(id.to_i)
                        }
                    }
                }
              }
          }
        )
      end
    end
    clear_count
  end

  # ----------------------------------------------------------------------
  # Metadata

  protected

  def _series_name
    @_series_name ||= "#{container.class.name.demodulize}:#{container.id}:#{name}".underscore
  end

  public
  def self.create_meta_table
    ddb.create_table(
        table_name: DynamoSeries.meta_table_name,
        attribute_definitions: [
            { attribute_name: DynamoSeries::META_TABLE_ATTR_SERIES_NAME,  attribute_type: 'S', },
            # { attribute_name: DynamoSeries::META_TABLE_ATTR_UPDATED_AT,   attribute_type: 'N', },
            # { attribute_name: DynamoSeries::META_TABLE_ATTR_COUNT,        attribute_type: 'N', },
        ],
        key_schema: [
            { attribute_name: DynamoSeries::META_TABLE_ATTR_SERIES_NAME,  key_type: 'HASH', },
        ],
        provisioned_throughput: {
            read_capacity_units: 1,
            write_capacity_units: 1_000,
        }
    )
  end

  # ----------------------------------------------------------------------
  # Count maintenance
  protected

  def update_count n=1
    ddb.update_item(
        table_name: meta_table_name,
        key: { META_TABLE_ATTR_SERIES_NAME => { 'S' => _series_name } },
        attribute_updates: {
            META_TABLE_ATTR_COUNT => { action: 'ADD',
                                       value: dmember_from_ruby(n.to_i) },
            META_TABLE_ATTR_UPDATED_AT => { action: 'PUT',
                                            value: dmember_from_ruby(Time.now.to_f) },
        }
    )
    Rails.cache.delete memcache_count_key if @memcache_count
  end

  def clear_count
    ddb.update_item(
        table_name: meta_table_name,
        key: { META_TABLE_ATTR_SERIES_NAME => { 'S' => _series_name } },
        attribute_updates: {
            META_TABLE_ATTR_COUNT => { action: 'PUT',
                                       value: dmember_from_ruby(0) }
        }
    )
    Rails.cache.delete memcache_count_key if @memcache_count
  end

  COUNT_THRESHOLD = 100

  def read_count
    memcache_if @memcache_count, memcache_count_key do
      calculate_count
    end
  end

  def calculate_count
    q = {
        table_name: table_name,
        key_conditions: {
            local_key => {
                attribute_value_list: [ dmember_from_ruby(container.id.to_i) ],
                comparison_operator: 'EQ'
            }
        },
        limit: COUNT_THRESHOLD,
        select: 'COUNT',
    }.merge_if(default_index, index_name: default_index.to_s)

    # First try to count them with a simple query
    resp = ddb.query(q)
    if !resp[:last_evaluated_key]
      return resp.count
    end

    # Otherwise get it from the count table
    resp = ddb.query(
        table_name: meta_table_name,
        key_conditions: {
            META_TABLE_ATTR_SERIES_NAME => {
                attribute_value_list: [{ 'S' => _series_name }],
                comparison_operator: 'EQ',
            }
        }
    )

    # If we know that we are > COUNT_THRESHOLD, but somehow end up <, then
    # just return COUNT_THRESHOLD + 1
    count = dmember_to_ruby(resp.member[0][META_TABLE_ATTR_COUNT]).to_i
    return (if count > COUNT_THRESHOLD
              count
            else
              COUNT_THRESHOLD + 1
            end)
  end

  # ----------------------------------------------------------------------
  # Memcache related

  def memcache_count_key
    @memcache_count_key ||= "Series:DynamoSeries:#{_series_name}:count"
  end
  def memcache_if boolean, key, &block
    if boolean
      Rails.cache.fetch key, &block
    else
      yield
    end
  end

  # ----------------------------------------------------------------------
  # Overrides
  public

  def first
    _report_attributes([super]).first
  end
  def last
    _report_attributes([super]).first
  end
  def to_a
    _report_attributes(super)
  end

  protected
  def _insert objs, prepend: false, inversion_disabled: false
    _extract_attributes(objs)
    super objs, prepend: prepend, inversion_disabled: inversion_disabled
  end
  def _insert_inverse objs, prepend: false
    if inverse_of
      objs.each do |obj|
        if inv = obj.attempt(inverse_of)
          inv.update_count 1
        end
      end
    end
  end
  def _delete objs, inversion_disabled: false
    super objs, inversion_disabled: inversion_disabled
  end
  def _delete_inverse objs
    if inverse_of
      objs.each do |obj|
        if inv = obj.attempt(inverse_of)
          inv.update_count -1
        end
      end
    end
  end

  # ----------------------------------------------------------------------
  # Internal utility
  private

  def ddb
    self.class.ddb
  end
  def self.ddb
    DynamoDB.current
  end

  def _report_attributes objs
    if @reportable_attributes && attributes_strategy
      objs.each do |obj|
        if obj && a = @reportable_attributes[obj.id.to_i]
          attributes_strategy.report_attr(obj, a.with_indifferent_access)
        end
      end
    end
    objs
  end
  def _extract_attributes objs
    @extracted_attributes = {}
    if attributes_strategy
      objs.each do |obj|
        @extracted_attributes[obj.id.to_i] = attributes_strategy.calculate_attr(obj)
      end
    end
    objs
  end

  # ------------------------------------------------------------------------
  # DynamoDB member representation ('N' => ...)

  def dmember_to_ruby dmember
    if dmember[:n]
      dmember[:n].to_f
    elsif dmember[:s]
      dmember[:s].to_s
    else
      raise "unsupported dmember #{dmember.inspect}"
    end
  end
  def dmembers_to_ruby hash
    hash.hmap{|k, v|
      [k.to_s, dmember_to_ruby(v)]
    }
  end
  def dmember_from_ruby ruby
    if ruby.kind_of? Numeric
      { 'N' => ruby.to_s }
    elsif ruby.kind_of? String
      { 'S' => ruby.to_s }
    else
      raise "unsupported conversion to dmember #{ruby.inspect}"
    end
  end
  def dmembers_from_ruby hash
    hash.hmap{|k, v|
      [k.to_s, dmember_from_ruby(v)]
    }
  end

end

end
