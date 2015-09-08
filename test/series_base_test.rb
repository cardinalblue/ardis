require_relative 'test_helper'
require_relative 'series_test_fixture'


class SeriesBaseTest < ActiveSupport::TestCase
  include Ardis
  include Ardis::RedisAdapter
  include Ardis::SeriesTestFixture


  # ---------------------------------------------------------------------------

  def t_list_g
    @t_list_g       ||= _t_setup RedisAdapter::ListSeries.new       relation: FeedEntry, container: FeedEntry,
                                                             name: :g_list
  end
  def t_set_g
    @t_set_g        ||= _t_setup RedisAdapter::SetSeries.new        relation: FeedEntry, container: FeedEntry,
                                                             name: :g_set
  end
  def t_sortedset_g
    @t_sortedset_g  ||= _t_setup RedisAdapter::SortedSetSeries.new  relation: FeedEntry, container: FeedEntry,
                                                             name: :g_sortedset, attr_score: :key
  end
  def t_list
    @t_list         ||= _t_setup RedisAdapter::ListSeries.new       relation: FeedEntry, container: @user,
                                                             name: :list
  end
  def t_sortedset
    @t_sortedset    ||= _t_setup RedisAdapter::SortedSetSeries.new  relation: FeedEntry, container: @user,
                                                             name: :sorted_set, attr_score: :key
  end
  def t_set
    @t_set          ||= _t_setup RedisAdapter::SetSeries.new        relation: FeedEntry, container: @user,
                                                             name: :set
  end
  def t_array
    @t_array        ||= _t_setup ArraySeries.new             relation: FeedEntry, container: @user,
                                                             name: :array_series
  end
  def t_serieses
    @t_serieses ||= [
        t_list_g,         t_list,          t_array,
        t_sortedset_g,    t_sortedset,
        t_set_g,          t_set
    ]
  end
  def t_serieses_orderable
    @t_serieses_orderable ||= [
        t_list_g,         t_list,         t_array,
        t_sortedset_g,    t_sortedset,
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

end
