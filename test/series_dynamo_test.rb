require 'test_helper'
require_relative 'series_base_test'

class SeriesDynamoTest < SeriesBaseTest

  # --------------------------------------------------------------------------
  # Setup

  def setup
    super
    setup_dynamo_meta_table
    setup_dynamo_data_table
  end

  # --------------------------------------------------------------------------
  test 'default attributes' do
    u = User.create
    series = DynamoSeries.new container: u,
                              relation: FeedEntry
    assert_equalities 'user_feed_entries',  series.table_name,
                      'user_id',            series.local_key,
                      'feed_entry_id',      series.foreign_key,
                      'feed_entries',       series.name

  end

  test_varying 'insertion and include read', extra_entries: [0, DynamoSeries::COUNT_THRESHOLD, DynamoSeries::COUNT_THRESHOLD-1] do |var|

    # Do this before the first time we call t_dynamo
    extras = var.extra_entries.times.map{ FeedEntry.create }
    @entries.push *extras

    # Initial
    assert_equalities @entries,       t_dynamo.index_by(:user_id_t_index).to_a,
                      @entries.count, t_dynamo.count

    t_dynamo << (e1 = FeedEntry.create)
    assert t_dynamo.include? e1
    assert_equalities @entries + [e1],    t_dynamo.index_by(:user_id_t_index).to_a,
                      @entries.count + 1, t_dynamo.count

    # Insert e1
    e2 = FeedEntry.create
    refute t_dynamo.include? e2
    t_dynamo << e2
    assert t_dynamo.include? e2
    assert_equalities @entries + [e1, e2], t_dynamo.index_by(:user_id_t_index).to_a,
                      @entries.count + 2, t_dynamo.count

    # Reinsert e1
    t_dynamo << e1
    assert t_dynamo.include? e1
    assert_equalities @entries + [e2, e1], t_dynamo.index_by(:user_id_t_index).to_a,
                      @entries.count + 2, t_dynamo.count

    # Delete e2
    t_dynamo.delete e2
    refute t_dynamo.include? e2
    assert_equalities @entries + [e1], t_dynamo.index_by(:user_id_t_index).to_a,
                      @entries.count + 1, t_dynamo.count

    # Delete entries
    t_dynamo.delete @entries
    assert_equalities [e1], t_dynamo.index_by(:user_id_t_index).to_a,
                      1, t_dynamo.count

  end

  test 'del' do
    assert t_dynamo.count > 0
    assert t_dynamo.to_a.present?

    t_dynamo.del
    assert_equalities [], t_dynamo.to_a,
                      0, t_dynamo.count

    t_dynamo << 50.times.map{ FeedEntry.create }
    assert t_dynamo.count > 0
    assert t_dynamo.to_a.present?

    t_dynamo.del
    assert_equalities [], t_dynamo.to_a,
                      0, t_dynamo.count
  end

  test 'multiple queries' do
    series = DynamoSeries.new name: :feed_entries, relation: FeedEntry,
                              container: User.create,
                              attributes: ->(_) { { 'filler' => ('x' * 64_000) } }
    entries = 20.times.map{ FeedEntry.create }
    series.push entries

    assert_equalities entries[5, 0].map(&:id),
                        series.offset(5).limit(0).map(&:id),
                      entries[5, 5].map(&:id),
                        series.offset(5).limit(5).map(&:id),
                      entries[5, 20].map(&:id),
                        series.offset(5).limit(20).map(&:id)

    # Delete
    entries[6] .destroy; entries[6]  = nil
    entries[15].destroy; entries[15] = nil

    assert_equalities entries[5, 0]               .map{|e| e.try(:id) },
                      series.offset(5).limit(0)   .map{|e| e.try(:id) },
                      entries[5, 5]               .map{|e| e.try(:id) },
                      series.offset(5).limit(5)   .map{|e| e.try(:id) },
                      entries[5, 20]              .map{|e| e.try(:id) },
                      series.offset(5).limit(20)  .map{|e| e.try(:id) }

    # Delete, autocompact
    entries.compact!
    assert_equalities entries[5, 0]                         .map{|e| e.try(:id) },
                      series.autocompact.offset(5).limit(0) .map{|e| e.try(:id) },
                      entries[5, 5]                         .map{|e| e.try(:id) },
                      series.autocompact.offset(5).limit(5) .map{|e| e.try(:id) },
                      entries[5, 20]                        .map{|e| e.try(:id) },
                      series.autocompact.offset(5).limit(20).map{|e| e.try(:id) }

  end

  test 'attribute setting' do
    e1 = FeedEntry.create my_accessor: 'E1'
    e2 = FeedEntry.create my_accessor: 'E2'

    series = DynamoSeries.new name: :feed_entries, relation: FeedEntry,
                              container: User.create,
                              attributes: ->(entry, attrs=nil) {
                                # Setter or getter
                                if attrs && attrs[:my_accessor]
                                  entry.my_accessor = attrs[:my_accessor]
                                else
                                  { my_accessor: entry.my_accessor }
                                end
                              }

    # Push, it should write the 'my_accessor'
    series.push e1, e2

    e1.reload
    e2.reload

    # Read again
    es = series.to_a
    assert_equalities [e1, e2].map(&:id), es.map(&:id),
                      %w[E1 E2], es.map(&:my_accessor)
    assert es[0].object_id != e1.object_id

  end

  # -------------------------------------------------------------------------

  test_varying 'total_count DynamoSeries with cache', memcache_count: [true,false] do |var|

    e1 = FeedEntry.create my_accessor: 'E1'
    e2 = FeedEntry.create my_accessor: 'E2'

    series1 = DynamoSeries.new name: :feed_entries, relation: FeedEntry,
                               container: User.create,
                               memcache_count: var.memcache_count
    series2 = DynamoSeries.new name: :feed_entries, relation: FeedEntry,
                               container: User.create,
                               memcache_count: var.memcache_count

    if var.memcache_count
      mock.proxy(series1).calculate_count.times(4)
      mock.proxy(series2).calculate_count.times(4)
    end

    assert_equal 0, series1.total_count
    assert_equal 0, series2.total_count

    series1.push e1
    assert_equal 1, series1.total_count
    assert_equal 1, series1.total_count
    assert_equal 0, series2.total_count
    assert_equal 0, series2.total_count

    series1.push e2
    assert_equal 2, series1.total_count
    assert_equal 2, series1.total_count
    assert_equal 0, series2.total_count

    series2.push e2
    assert_equal 2, series1.total_count
    assert_equal 2, series1.total_count
    assert_equal 1, series2.total_count

    series1.delete e2
    assert_equal 1, series1.total_count
    assert_equal 1, series1.total_count
    assert_equal 1, series2.total_count

    series2.push e1
    assert_equal 1, series1.total_count
    assert_equal 1, series1.total_count
    assert_equal 2, series2.total_count

    series2.del
    assert_equal 1, series1.total_count
    assert_equal 1, series1.total_count
    assert_equal 0, series2.total_count

  end


end