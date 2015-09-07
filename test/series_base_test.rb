require 'test_helper'
require_relative 'series_test_fixture'


class SeriesBaseTest < ActiveSupport::TestCase
  include Ardis
  include Ardis::Redis
  include Ardis::SeriesTestFixture


  DynamoSeries = Ardis::DynamoSeries

  # Temporary
  # class DynamoSeries < ArraySeries
  # end

  # ---------------------------------------------------------------------------

  def t_list_g
    @t_list_g       ||= _t_setup Redis::ListSeries.new       relation: FeedEntry, container: FeedEntry,
                                                             name: :g_list
  end
  def t_set_g
    @t_set_g        ||= _t_setup Redis::SetSeries.new        relation: FeedEntry, container: FeedEntry,
                                                             name: :g_set
  end
  def t_sortedset_g
    @t_sortedset_g  ||= _t_setup Redis::SortedSetSeries.new  relation: FeedEntry, container: FeedEntry,
                                                             name: :g_sortedset, attr_score: :key
  end
  def t_list
    @t_list         ||= _t_setup Redis::ListSeries.new       relation: FeedEntry, container: @user,
                                                             name: :list
  end
  def t_sortedset
    @t_sortedset    ||= _t_setup Redis::SortedSetSeries.new  relation: FeedEntry, container: @user,
                                                             name: :sorted_set, attr_score: :key
  end
  def t_set
    @t_set          ||= _t_setup Redis::SetSeries.new        relation: FeedEntry, container: @user,
                                                             name: :set
  end
  def t_array
    @t_array        ||= _t_setup ArraySeries.new             relation: FeedEntry, container: @user,
                                                             name: :array_series
  end
  def t_dynamo
    @t_dynamo       ||= begin
                          setup_dynamo
                          _t_setup DynamoSeries.new          relation: FeedEntry, container: @user,
                                                             name: :feed_entries,
                                                             default_index: :user_id_t_index
                        end
  end
  def t_serieses
    @t_serieses ||= [
        t_list_g,         t_list,          t_array,
        t_sortedset_g,    t_sortedset,     t_dynamo,
        t_set_g,          t_set
    ]
  end
  def t_serieses_orderable
    @t_serieses_orderable ||= [
        t_list_g,         t_list,         t_array,
        t_sortedset_g,    t_sortedset,    t_dynamo
    ]
  end
  def _t_setup series
    series << @entries
  end

  # -----------------------------------------------------------------------
  def setup
    super

    # Create test ActiveRecord classes
    ActiveRecord::Migration.verbose = false
    Migration.up

    # -----------------------------------------
    # Test fixures

    @user = User.create
    @entries = 10.times.collect{ FeedEntry.create(feed_entry_detail: FeedEntryDetail.create) }

  end

  def teardown
    super
    Migration.down
    ::Redis.current.flushdb
  end

  # -----------------------------------------------------------------------

  def setup_dynamo
    setup_dynamo_meta_table
    setup_dynamo_data_table
  end

  def setup_dynamo_meta_table
    dynamo_table_delete! DynamoSeries::META_TABLE_NAME if dynamo_table_exists? DynamoSeries::META_TABLE_NAME
    DynamoSeries.create_meta_table
  end

  def setup_dynamo_data_table
    table_name = 'user_feed_entries'
    dynamo_table_delete! table_name if dynamo_table_exists? table_name
    ddb.create_table(
        table_name: table_name,
        attribute_definitions: [
            { attribute_name: 'user_id',        attribute_type: 'N', },
            { attribute_name: 'feed_entry_id',  attribute_type: 'N', },
            { attribute_name: 't',              attribute_type: 'N', },
        ],
        key_schema: [
            { attribute_name: 'user_id',        key_type: 'HASH', },
            { attribute_name: 'feed_entry_id',  key_type: 'RANGE', },
        ],
        global_secondary_indexes: [
            {
                index_name: 'feed_entry_id_t_index',
                key_schema: [
                    { attribute_name: 'feed_entry_id',  key_type: 'HASH', },
                    { attribute_name: 't',              key_type: 'RANGE', },
                ],
                projection: { projection_type: 'ALL' },
                provisioned_throughput: {
                    read_capacity_units: 1,
                    write_capacity_units: 1_000,
                }
            },
            {
                index_name: 'user_id_t_index',
                key_schema: [
                    { attribute_name: 'user_id',  key_type: 'HASH', },
                    { attribute_name: 't',        key_type: 'RANGE', },
                ],
                projection: { projection_type: 'ALL' },
                provisioned_throughput: {
                    read_capacity_units: 1,
                    write_capacity_units: 1_000,
                }
            },
        ],
        provisioned_throughput: {
            read_capacity_units: 1,
            write_capacity_units: 1_000,
        }
    )
  end

  test 't_dynamo' do
    assert_equalities 'user_feed_entries', t_dynamo.table_name
  end

  # --------------------------------------------------------------------------
  private
  def ddb
    @ddb ||= DynamoDB.current
  end

  def dynamo_table_exists? table_name
    ddb.describe_table(table_name: table_name)
    return true
  rescue AWS::DynamoDB::Errors::ResourceNotFoundException
    return false
  end

  def dynamo_table_delete! table_name
    ddb.delete_table(table_name: table_name)
  end


end
