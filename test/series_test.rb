require_relative 'test_helper'
require_relative 'series_base_test'

class SeriesTest < SeriesBaseTest

  # -------------------------------------------------------------------------
  # all

  test_varying 'list-like runthrough', klass: [ ListSeries, ArraySeries ] do |var|

    series = var.klass.new name: :feed_entries, relation: FeedEntry, container: User.create

    assert_equal [], series.to_a

    series << (f1 = FeedEntry.create)
    assert_equal [ f1 ], series.to_a

    series.push (f2 = FeedEntry.create), (f3 = FeedEntry.create).id
    assert_equal [ f1, f2, f3 ], series.to_a

    series.delete f2
    assert_equal [ f1, f3 ].map(&:id),          series.to_a.map(&:id)
    assert_equal [ f3 ],                        series.offset(1).to_a
    assert_equal [ f1 ],                        series.offset(1).reverse_order.to_a
    assert_equal [ f1 ],                        series.limit(1).to_a
    assert_equal [ f3 ],                        series.limit(1).reverse_order.to_a

    series.unshift (f4 = FeedEntry.create), (f5 = FeedEntry.create).id
    assert_equal [ f5, f4, f1, f3 ].map(&:id),  series.to_a.map(&:id)
    assert_equal [ f1, f3 ],                    series.offset(2).to_a
    assert_equal [ f1, f4, f5 ],                series.offset(1).reverse_order.to_a
    assert_equal [ f1, f4 ],                    series.offset(1).limit(2).reverse_order.to_a
    assert_equal [ f1 ],                        series.offset(2).limit(1).to_a

  end

  test 'to_a' do
    t_serieses.each{ |series|
      result = series.offset(3).limit(5).to_a

      case series
      when SortedSetSeries
        assert_equal @entries.map(&:key).sort[3, 5], 
                     result.map(&:key).map(&:to_i), series.name 
      when SetSeries
        assert_equal 5, result.count
        assert_subset @entries, result, series.name.to_s
      when ListSeries, ArraySeries
        assert_equal @entries[3, 5], result, series.name
      end
    }
  end

  test 'reverse_order.to_a' do
    t_serieses.each{ |series|
      result = series.offset(3).limit(5).reverse_order.to_a

      case series
      when SortedSetSeries
        assert_equal @entries.map(&:key).sort.reverse[3, 5], 
                     result.map(&:key).map(&:to_i), series.name
      when SetSeries
        assert_equal 5, result.count
        assert_subset @entries, result, series.name.to_s
      when ListSeries, ArraySeries
        assert_equal @entries.reverse[3, 5], result, series.name
      end
    }
  end

  test_varying 'offset', offset_mult: [0, 0.5, 1, 1.2],
                         limit_mult: [nil, 0.5, 1, 1.2],
                         reverse: [true,false],
                         series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    total = series.count

    offset   = Integer(total * var.offset_mult)
    limit    = var.limit_mult && Integer(total * var.limit_mult)
    s        = series.offset(offset)
    s        = s.limit(limit) if limit
    s        = s.reverse_order if var.reverse
    @entries = @entries.reverse if var.reverse

    expected = @entries[offset, limit || 1000] || []
    assert_equal expected, s.to_a
  end

  test_varying 'offset from empty',
               klass: [ ListSeries, SortedSetSeries, SetSeries, ArraySeries ],
               offset: [nil, 0, 4],
               limit: [nil, 1, 4] do |var|

    series = var.klass.new name: :feed_entries, relation: FeedEntry, container: User.create
    assert_equal [], series.offset(var.offset).limit(var.limit).to_a
    assert_equal [], series.offset(var.offset).limit(var.limit).reverse_order.to_a
  end

  test_varying 'limit 0 always empty', offset: [0, 3], reverse_order: [true,false]  do |var|
    t_serieses.each do |series|
      assert_equal [], series.offset(var.offset).limit(0).reverse_order(var.reverse_order).to_a
    end
  end

  # -------------------------------------------------------------------------
  # Relation setting

  class RelationContainer
    include Ardis::SeriesTestFixture
    def private_users
      User.where(privacy_mode: User::PrivacyPrivate)
    end
  end

  test_varying 'relation values', 
    relation: [ '->(c) { c.private_users }', 
                ':private_users', 
                'User.where(privacy_mode: User::PrivacyPrivate)',
                'User',
                ] do |var|

    relation = eval var.relation
    series = ListSeries.new name: 'relationtest', key: 'relationtest',
                            container: RelationContainer.new,
                            relation: relation

    series.push [
      User.create(privacy_mode: User::PrivacyPublic),
      User.create(privacy_mode: User::PrivacyPrivate),
      User.create(privacy_mode: User::PrivacyPrivate) ]

    if relation == User
      assert_equal 3, series.to_a.count
    else
      assert_equal 2, series.autocompact.to_a.count
      assert series.to_a.all?{|u| u.privacy_mode == User::PrivacyPrivate}
    end
  end

  # -------------------------------------------------------------------------
  # Order

  test_varying 'SortedSet order and offset', klass:    [ ListSeries, SortedSetSeries ] do |var|

    entries = 200.times.map{ FeedEntry.create }.sample(100)

    s = if var.klass == ListSeries
          ListSeries.new name: :testlist, relation: FeedEntry
        elsif var.klass == SortedSetSeries
          SortedSetSeries.new name: :testlist, relation: FeedEntry, attr_score: :key
        end
    s.push *entries

    if var.klass == SortedSetSeries
      entries = entries.sort_by &:key
    end

    assert_equal entries[2, 10],               s.offset(2).limit(10).to_a
    assert_equal entries[2],                   s.offset(2).first
    assert_equal entries[-3],                  s.offset(2).last

    assert_equal entries.reverse[2, 10],       s.offset(2).limit(10).reverse_order.to_a
    assert_equal entries.reverse[2, 10].first, s.offset(2).reverse_order.first
    assert_equal entries.reverse[-3],          s.offset(2).reverse_order.last

  end

  # -------------------------------------------------------------------------
  # Push, unshift

  test 'push empty' do
    t_serieses.each do |s|
      assert_no_difference 's.updated_at.to_f', 's.total_count' do
        s.push 
        s.push nil, nil, nil
      end
    end
  end

  test_varying 'push obj or ids on list', series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    series.push *10.times.map{ FeedEntry.create }
    series.push *10.times.map{ FeedEntry.create.id }
    assert_equal FeedEntry.last(20).reverse.map(&:id), series.reverse_order.limit(20).to_a.map(&:id)
  end

  test 'push ids on sorted set' do
    s = SortedSetSeries.new name: :sorted_set, relation: FeedEntry, 
                            attr_score: ->(_id){ -_id }
      # Since we'll be passing ids, the score will be negative of the id
    s.del
    s.push *10.times.map{ FeedEntry.create.id }
    assert_equal FeedEntry.last(10).reverse, s.to_a
      # The negative score reverses them
  end

  test_varying 'list/array push', series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    new_entry = FeedEntry.create
    series << new_entry
    assert_equal new_entry, series.reverse_order.first
    assert_equal @entries.count + 1, series.to_a.count
    assert_equal @entries.count + 1, series.total_count
  end

  test_varying 'list/array unshift', series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    new_entry = FeedEntry.create
    series.unshift new_entry
    assert_equal new_entry, series.first
    assert_equal @entries.count + 1, series.to_a.count
    assert_equal @entries.count + 1, series.total_count
  end

  test_varying 'list/array shift 1', series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    preshifted = series.offset(0).limit(1).to_a[0]
    shifted    = series.shift
    assert_equalities 9,          series.count,
                      preshifted, shifted

  end

  test_varying 'list shift n', n: [0, 3, 20], series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    preshifted = series.offset(0).limit(var.n).to_a
    shifted    = series.shift var.n
    assert_equalities [var.n, 10].min,      shifted.count,
                      [10 - var.n, 0].max,  series.count,
                      preshifted,           shifted

  end

  test 'sorted set push, first, last' do
    new_entries = [ FeedEntry.create, FeedEntry.create ]
    t_sortedset.push *new_entries

    all_entries_unsorted = @entries + new_entries
    all_entries = all_entries_unsorted.sort_by &:key

    assert_not_equal all_entries_unsorted, all_entries
    assert_equal all_entries.first, t_sortedset.first
    assert_equal all_entries.last, t_sortedset.last
    assert_equal all_entries, t_sortedset.to_a
  end

  test 'sorted set push an existing element' do
    last = t_sortedset.last

    assert_not_updated 't_sortedset.updated_at' do
      t_sortedset << last
    end

    assert_equal last, t_sortedset.last

    assert_updated 't_sortedset.updated_at' do
      t_sortedset << FeedEntry.create
    end
  end

  # -------------------------------------------------------------------------
  # First/Last

  test_varying 'list/array first', series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    assert_equal @entries.first, series.first
    assert_equal @entries,       series.to_a  # `.to_a` should still be everything
  end

  test_varying 'list/array first autocompacting', series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    2.times do
      @entries.delete_at(0).destroy
    end
    assert_equal @entries.first, series.autocompact.first
  end

  test_varying 'list last', series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    assert_equal @entries.last, series.last
    assert_equal @entries, series.to_a  # `.to_a` should still be everything
  end

  test 'incr' do
    entry = @entries.last
    [ t_sortedset_g, t_sortedset ].each{ |sorted_set|
      assert_difference "sorted_set[entry.id]" do
        sorted_set.incr(entry)
      end
    }
  end

  # -------------------------------------------------------------------------
  # includes, select and where

  test 'includes loads associations' do
    t_serieses.each{ |series|
      series.includes(:user).each{ |collage|
        assert collage.association(:user).loaded?
        refute collage.association(:feed_entry_detail).loaded?
      }
    }
  end

  test 'where' do
    user_entries = @entries.sample(5)
    user_entries.each{ |e| e.update_attributes user: @user }

    t_serieses.each{ |series|
      assert series.where(user_id: @user.id).to_a
                   .all?{ |entry| entry.nil? || @user == entry.user }
    }
  end

  test 'select' do
    [t_list, t_array, t_sortedset, t_set].each{ |series|
      series.select(:id).select(:key, :created_at).each{ |entry|
        assert entry.id
        assert entry.key
        assert entry.created_at
        refute entry.respond_to?(:updated_at)
      }
    }
  end

  # -------------------------------------------------------------------------
  # Delete

  test_varying 'delete empty', series: %w[ t_list t_sortedset t_set ] do |var|
    s = eval var.series
    assert_equal 0, s.delete([])
  end

  test_varying 'delete', series: %w[ t_list t_sortedset t_set ] do |var|
    new_entry = FeedEntry.create

    s = eval var.series
    s.push new_entry
    assert s.include?(new_entry)
    assert_difference 's.redis_obj.size', -1 do
      s.delete new_entry
    end
    assert !s.include?(new_entry)
  end

  # -------------------------------------------------------------------------
  # include?

  test_varying 'include?', series: %w[ t_list t_array t_sortedset t_set ] do |var|
    new_entry = FeedEntry.create
    s = eval var.series
    refute s.include?(new_entry)
    s.push new_entry
    assert s.include?(new_entry)
    assert s.include?(new_entry.id)
    refute s.include?(23232323)

    fake_obj = Struct.new(:id).new new_entry.id
    assert !s.include?(fake_obj)
  end

  test_varying 'include? checks existence', series: %w[ t_list t_array t_sortedset t_set ] do |var|
    entry = @entries[0]
    s = eval var.series
    s << entry
    entry.destroy
    assert !s.include?(entry)
  end

  test_varying 'include? with offsets and limits', series: %w[t_list t_array t_sortedset] do |var|
    s = eval var.series
    entries = @entries
    entries = entries.sort_by &:key if s.kind_of? SortedSetSeries
    assert !s.offset(1).limit(2).include?(entries[0])
    assert  s.offset(1).limit(2).include?(entries[1])
    assert  s.offset(1).limit(2).include?(entries[2])
    assert !s.offset(1).limit(2).include?(entries[3])
  end

  # -------------------------------------------------------------------------
  # exists?

  test_varying 'exists?', klass: [ ListSeries, SortedSetSeries ] do |var|
    s = var.klass.new relation: FeedEntry, key: 'test_exists'
    refute s.exists?
    refute s.exists?  # Check that checking doesn't itself create it

    s << FeedEntry.create
    assert s.exists?

    s.del
    refute s.exists?
  end

  # -------------------------------------------------------------------------
  # Sample

  test_varying 'sample', series:%w[ t_list t_array t_sortedset t_set ] do |var|
    s   = eval var.series
    all = s.to_a

    assert_subset all,          s                   .sample(3)
    assert_subset all[5..-1],   s.offset(5)         .sample(3)
    assert_subset all[3..5],    s.offset(3).limit(3).sample(3)
    assert_subset all[3..6],    s.offset(3).limit(4).sample(2)
    assert_subset all[3..6],    s.offset(3).limit(4).sample(20)

    assert_equalities 3, s                    .sample(3)    .count,
                      2, s.limit(2)           .sample(3)    .count,
                      2, s.limit(3)           .sample(2)    .count,
                      3, s.offset(7)          .sample(10)   .count,
                      2, s.offset(7).limit(2) .sample(10)   .count

    assert_equalities 3, s                    .sample(3)    .to_a.count,
                      2, s.limit(2)           .sample(3)    .to_a.count,
                      2, s.limit(3)           .sample(2)    .to_a.count,
                      3, s.offset(7)          .sample(10)   .to_a.count,
                      2, s.offset(7).limit(2) .sample(10)   .to_a.count

  end

  # -------------------------------------------------------------------------
  # Connection

  test 'with_redis' do
    assert_equal ::Redis.current, t_list.send(:actual_redis_obj).redis

    r = ::Redis.new url: 'redis://fake.url.com'
    assert_equal r, t_list.with_redis(r).send(:actual_redis_obj).redis
  end

  # -------------------------------------------------------------------------
  # Attributes

  test_varying 'count', series: %w[ t_list t_array ] do |var|
    series = eval(var.series)
    assert_equal 5, series.offset(5).count
    assert_equal 3, series.offset(5).limit(3).count
    assert_equal 0, series.offset(11).count
  end

  test_varying 'updated_at', klass: [ ListSeries, SortedSetSeries ] do |var|

    series = var.klass.new(name: :feed_entries, relation: FeedEntry, container: User.create)
    assert_equalities nil, series.updated_at
    assert_updated 'series.updated_at' do
      series << FeedEntry.create
    end
    assert_updated 'series.updated_at' do
      series << FeedEntry.create
    end
  end

  test_varying 'updated_at', series: %w[ t_list t_array t_sortedset t_set ],
                             method: %i[ << push unshift delete ] do |var|
    t = rand(100000000)
    stub(Time).now{ Time.at t }

    s = eval var.series
    e = FeedEntry.create
    s.push e if var.method == :delete  # Otherwise delete fails and doesn't updated_at
    s.send var.method, e
    assert_equal t, s.updated_at.to_i
  end  

  test_varying 'empty?', klass: [ ListSeries, ArraySeries ] do |var|

    series = var.klass.new(name: :feed_entries, relation: FeedEntry, container: User.create)
    assert series.empty?
    series << FeedEntry.create
    refute series.empty?    
  end

  test 'total_count' do
    t_serieses.each{ |series|
      assert_equal 10, series.total_count
    }
  end

  class KeyTestClass < Struct.new(:id); end
  test 'key' do
    def key(series)
      series.send :actual_key
    end
    container = KeyTestClass.new(100)

    # Instance series
    series = ListSeries.new(name: :t1, container: container, relation: FeedEntry)
    assert_equal "series_test/key_test_class:100:t1", key(series)
    series.key = "bangbang";     assert_equal "bangbang", key(series)
    series.key = ->(_){ "foo" }; assert_equal "foo",      key(series)

    # Global series
    series = ListSeries.new(name: :t1, container: KeyTestClass, relation: FeedEntry)
    assert_equal "series_test/key_test_class::t1", key(series)

  end

  # -------------------------------------------------------------------------
  # DSL

  test 'dsl global' do
    assert_equalities ListSeries,                User.my_global_list.class,
                      :my_global_list,           User.my_global_list.name,
                      :my_global_list_key,       User.my_global_list.redis_obj.key
    assert_equalities SortedSetSeries,           User.my_global_sorted_set.class,
                      :my_global_sorted_set,     User.my_global_sorted_set.name,
                      :my_global_sorted_set_key, User.my_global_sorted_set.redis_obj.key
    assert_equalities SetSeries,                 User.my_global_set.class,
                      :my_global_set,            User.my_global_set.name,
                      :my_global_set_key,        User.my_global_set.redis_obj.key
  end

  test 'dsl' do  
    u = User.create
    assert_equalities ListSeries,                u.my_list.class,
                      :my_list,                  u.my_list.name,
                      "list>#{u.id}",            u.my_list.redis_obj.key
    assert_equalities SortedSetSeries,           u.my_sorted_set.class,
                      :my_sorted_set,            u.my_sorted_set.name,
                      "sorted_set>#{u.id}",      u.my_sorted_set.redis_obj.key
    assert_equalities SetSeries,                 u.my_set.class,
                      :my_set,                   u.my_set.name,
                      "set>#{u.id}",             u.my_set.redis_obj.key
  end

  test 'dsl redis_opt maxlength' do
    u = User.create
    entries = 200.times.map{
      entry = FeedEntry.create
      u.my_list << entry
      entry
    }
    assert_equal 200, entries.size
    assert_equal 128, u.my_list.redis_obj.size
  end

  # -------------------------------------------------------------------------
  # series_reference

  test 'series_for' do
    assert User.series_for_my_list.kind_of?(BaseSeries)
  end


  test 'series_reference' do
    u = User.create
    entries = 6.times.map{
      e = FeedEntry.create user: u
      u.referenced_list << e
      e
    }
    expected = u.referenced_list.to_a
    assert_db_queries 1 do
      assert_equal expected, entries[1].user_referenced_list.to_a
    end
  end


  # -------------------------------------------------------------------------
  # Initializer

  test_varying 'initializer', first: [:count, :to_a] do |var|

    # Make sure Redis objs deleted
    u = User.create
    u.my_list.redis_obj.del
    u.my_sorted_set.redis_obj.del

    u = User.last
    assert_equal 10, u.my_list.count if var.first == :count        # Triggers initializer
    entries = u.my_list.to_a                                       # Triggers initializer
    assert_equal FeedEntry.last(10), entries

    assert_equal 10, u.my_sorted_set.count if var.first == :count  # Triggers initializer
    entries = u.my_sorted_set.to_a                                 # Triggers initializer
    assert_equal FeedEntry.last(10).sort_by(&:key), u.my_sorted_set.to_a

  end

  test 'initializer atomic' do

    series_opts = {
      key: :initializer_atomic_test,
      relation: User,
      attr_score: ->(u) { u.id },
      seqnum: true
    }

    # Benchmark create as Series and read it, then DELETE it
    initializer = ->(_) {
      %w[a b c].map{|name| User.create name: name }
    }
    series = SortedSetSeries.new **series_opts.merge(initializer: initializer)
    assert_equal %w[a b c], series.to_a.map(&:name)
    last_updated_at = series.updated_at
    last_seqnum     = series.seqnum

    # Delete the Redis key
    series.del

    sleep 0.05

    # Benchmark create same Series with race condition initializer
    initializer = ->(_) {
      # Simulated RACE CONDITION: we write to the same key
      sorted_set = ::Redis::SortedSet.new(series_opts[:key])
      %w[d e f g].each{|name|
        u = User.create name: name
        sorted_set[u.id] = u.id
      }

      # Return what we will try to initialize with (but should fail)
      %w[h i j k l].map{|name| User.create name: name }
    }
    series = SortedSetSeries.new **series_opts.merge(initializer: initializer)

    # Compare what was created
    assert_equal %w[d e f g],     series.to_a.map(&:name)
    assert_equal last_updated_at, series.updated_at
    assert_equal last_seqnum,     series.seqnum

  end

  test 'initializer empty' do

    series_opts = {
        key: :initializer_atomic_test,
        relation: User,
        attr_score: ->(u) { u.id },
        seqnum: true
    }

    # Benchmark create as Series and read it, then DELETE it
    initializer = ->(_) { nil }
    series = SortedSetSeries.new **series_opts.merge(initializer: initializer)
    assert_equal 0, series.count
  end

  test 'initializer can return ids' do

    series_opts = {
        key: :initializer_return_ids,
        relation: User,
        attr_score: ->(u) { Time.now },
        seqnum: true
    }
    initializer = ->(_) {
      10.times.map{|i|
        u = User.create name: "#{i}name"
        i < 5 ? u : u.id
      }
    }
    series = SortedSetSeries.new **series_opts.merge(initializer: initializer)

    # Compare what was created
    assert_equal 10.times.map.to_a, series.to_a.map(&:name).map(&:to_i)
  end

    # -------------------------------------------------------------------------
  test 'deleted objects' do

    # Make sure Redis objs deleted
    u = User.create
    u.my_list.redis_obj.del
    u.my_sorted_set.redis_obj.del

    u = User.last

    assert_equal 10, u.my_list.count  # Triggers initializer 
    entries = FeedEntry.last(10)
    assert_equal entries, u.my_list.to_a
    entries[2].destroy; entries[2] = nil;
    entries[4].destroy; entries[4] = nil;
    assert_equal entries, u.my_list.to_a

    assert_equal 10, u.my_sorted_set.count  # Triggers initializer 
    entries = FeedEntry.last(10).sort_by(&:key)
    assert_equal entries, u.my_sorted_set.to_a
    entries[2].destroy; entries[2] = nil;
    entries[4].destroy; entries[4] = nil;
    assert_equal entries, u.my_sorted_set.to_a

  end

  # -------------------------------------------------------------------------
  # Score

  test_varying 'attr_score', attr_score: [ 
                              ":key", 
                              "->(entry){ entry.key }" 
                              ] do |var|
    entries = 10.times.map{ FeedEntry.create }
    sorted_set = SortedSetSeries.new name: :attr_score_test, 
                                     container: @user, relation: FeedEntry, 
                                     attr_score: eval(var.attr_score)
    sorted_set.push *entries
    assert_equal entries.sort_by(&:key), sorted_set.to_a
  end

  test_varying 'score_for', with_scores: [ nil, true, false ],
                            attr_score: [ 
                              ":key", 
                              "->(entry){ entry.key }" 
                            ] do |var|
    entries = 10.times.map{ FeedEntry.create }
    sorted_set = SortedSetSeries.new name: :attr_score_test, 
                                     container: @user, relation: FeedEntry, 
                                     attr_score: eval(var.attr_score)
    sorted_set.push *entries

    # Set expectation of how many times the `score` method (which retrieves
    # the score from Redis) will be called. If caching is working correctly,
    # and enabled (with `with_scores`) then it will not be called at all).
    #
    num_times_score = var.with_scores ? 0 : entries.count
    any_instance_of(::Redis::SortedSet) do |redis_obj|
      mock.proxy(redis_obj).score.with_any_args.times(num_times_score)
    end

    # To exercise the cache functionality, do a read with_scores, or not
    if var.with_scores != nil  # nil means no reading at all
      sorted_set.with_scores(var.with_scores).to_a
    end

    # Check that `score_for` returns the correct thing
    entries.each{|e| 
      assert_equal e.key.to_f, sorted_set.score_for(e)
    }
  end

  class ScoredFeedEntry < FeedEntry
    attr_accessor :keyMOD
    def keyMOD
      @keyMOD ||= key * rand(1000)
    end
  end
  test_varying 'score_for assignment', 
                            attr_score: [ 
                              ':keyMOD',
                              '->(entry, val=nil) {
                                if val then entry.keyMOD = val
                                else entry.keyMOD
                                end
                              }'
                            ] do |var|
    entries = 10.times.map{ ScoredFeedEntry.create }
    sorted_set = SortedSetSeries.new name: :attr_score_test, 
                                     container: @user, relation: ScoredFeedEntry, 
                                     attr_score: eval(var.attr_score)
    sorted_set.push *entries
    entries = entries.sort_by(&:keyMOD)

    # Read back entries, make sure read correctly
    retrieved_entries = sorted_set.with_scores.to_a
    assert_equal entries, retrieved_entries

    # Check that keyMOD got set correctly (it's an in-memory value so it will
    # only match if the Series set it).
    #
    entries.zip(retrieved_entries).each{|entry, retrieved_entry| 
      assert_equal entry.keyMOD, retrieved_entry.keyMOD
    }

    # Also check first/last
    assert_equal entries.first.keyMOD, sorted_set.with_scores.first.keyMOD
    assert_equal entries.last.keyMOD,  sorted_set.with_scores.last.keyMOD
  end

  # -------------------------------------------------------------------------
  # index_of

  test 'index_of' do
    sorted_set = SortedSetSeries.new name: :rank_test,
                                     relation: FeedEntry,
                                     attr_score: ->(_id){ rand }

    sorted_set.push *(10.times.map{ FeedEntry.create })

    a = sorted_set.to_a
    a.each_with_index{ |e, index|
      assert_equal index, sorted_set.index_of(e)
    }

    a.reverse!
    a.each_with_index{ |e, index|
      assert_equal index, sorted_set.reverse_order.index_of(e)
    }
  end

  # -------------------------------------------------------------------------
  # Redis options

  test 'maxlength' do
    s = ListSeries.new name: :maxlength_test, relation: FeedEntry, redis_opts: { maxlength: 10 }
    entries = 100.times.map{ FeedEntry.create }    
    entries.each{|e| s << e }
    assert_equal entries[-10, 10], s.to_a
  end

  test 'expiration' do
    # Details:
    # https://redis.io/commands/expire
    # https://github.com/nateware/redis-objects#expiration
    #
    s = ListSeries.new name: :expiration_test, relation: FeedEntry, expiration: 1
    s << FeedEntry.create
    assert s.exists?
    sleep 1.01
    refute s.exists?
  end

  test 'expiration with initializer' do
    s = ListSeries.new name: :exp_with_init_test, relation: FeedEntry, expiration: 1,
      initializer: -> (_) { [ FeedEntry.create, FeedEntry.create ] }
    assert_equal 2, s.count
    sleep 1.01
    refute s.exists?
  end
  # -------------------------------------------------------------------------
  # ---- Kaminari behavior
  if (Kaminari rescue nil)

    test 'kaminari behavior' do

      # Setup
      s = User.my_global_list
      s.redis_obj.del
      entries = 100.times.map{ FeedEntry.create }
      s.push *entries

      # Tests
      assert_equal 3, s.page(3).current_page

      assert_equal entries[15, 5], s.page(4).per(5).to_a
      assert_equal 20, s.page(2).per(5).total_pages
      assert_equal 12, s.page(2).per(9).total_pages

      assert  s.page(1) .per(5).first_page?
      assert !s.page(2) .per(5).first_page?
      assert  s.page(20).per(5) .last_page?
      assert  s.page(10).per(10).last_page?
      assert !s.page(1) .per(5) .last_page?
      assert !s.page(19).per(5) .last_page?

      assert_equal entries[13, 4], s.page(3).per(4).padding(5).to_a

      assert_equal entries[18, 9], s.page(3).to_a  # Default is set in User
    end

    test 'kaminari empty' do

      # Setup
      s = User.my_global_list
      s.redis_obj.del
      f = FeedEntry.create

      # Make it an empty
      s.push f
      s.delete f

      # Tests
      assert_equal [], s.redis_obj.values
      assert s.page(1).per(10).first_page?
      assert s.page(1).per(10).last_page?
      assert !s.page(2).per(10).first_page?
      assert s.page(2).per(10).last_page?
    end

  end

  class Liker < User
    self.table_name = superclass.table_name
    include Ardis
  end
  class Likeable < FeedEntry
    self.table_name = superclass.table_name
    include Ardis
  end


  # -------------------------------------------------------------------------
  # inverse_of

  test_varying 'inverse_of', klass: [ ListSeries, SortedSetSeries ] do |var|

    Liker.define_series series_class: var.klass,
      name: :liked_entries, relation: Likeable, inverse_of: :likers
    Likeable.define_series series_class: var.klass,
      name: :likers, relation: Liker, inverse_of: :liked_entries

    entries = 20.times.map{ Likeable.create }.shuffle
    u1 = Liker.create

    # Like some entries
    liked_entries = entries.sample 5

    # First insert and check invert
    u1.liked_entries.push *liked_entries
    assert_equal_sets liked_entries, u1.liked_entries.to_a
    liked_entries.each do |entry| 
      assert_equal [u1], entry.likers.to_a
    end

    # Then delete and check invert
    u1.liked_entries.delete *liked_entries
    assert_equal [], u1.liked_entries.to_a
    liked_entries.each do |entry| 
      assert_equal [], entry.likers.to_a
    end

  end


  # -------------------------------------------------------------------------
  # ---- autocompact

  test_varying 'autocompact', klass: [ ListSeries, SortedSetSeries ],
                                  autocompact: [ true, false ],
                                  num_delete: [ 10, 40 ] do |var|

    # Setup base entries and Series
    entries = 100.times.map{ FeedEntry.create }.sample(60).shuffle
    s = var.klass.new name: :test_autocompact, relation: FeedEntry
    if SortedSetSeries === s
      s.attr_score = :key 
      entries = entries.sort_by &:key
    end
    s.push *entries  # AFTER we set the attr_score

    # Delete some of them
    deleted = entries.sample(var.num_delete)
    FeedEntry.delete(deleted)
    if var.autocompact
      entries -= deleted
    else
      # Replace in-place with `nil`
      entries = entries.map{|e| deleted.include?(e)? nil: e }
    end

    # We can't check the arrays exactly, since the compacting algorithm throws
    # off the offsets! So, just check that we get valid subarrays and objects
    #
    s = s.autocompact if var.autocompact
    assert_array_contains?(entries,           s.offset(2).limit(10).to_a)
    assert_array_contains?(entries.reverse,   s.offset(2).limit(10).reverse_order.to_a)
    assert_include? entries,                  s.offset(2).limit(10).first
    assert_include? entries,                  s.offset(2).limit(10).last
    assert_include? entries,                  s.offset(2).limit(10).reverse_order.first
    assert_include? entries,                  s.offset(2).limit(10).reverse_order.last
  end

  test_varying 'autocompact with where', klass: [ ListSeries, SortedSetSeries ] do |var|

    # Create 10 users, in which 5 are private
    users         = 10.times.map{ User.create }
    users_private = users.sample(5).each{|u| u.update_attributes privacy_mode: User::PrivacyPrivate }
    users_public  = users - users_private

    # Create series
    s = var.klass.new name: "test_autocompact#{var.klass}",
                      relation: User
    s.push *users

    # Query with `where` and assert that
    # users that do not satisfy are removed from Redis
    results = s.where('privacy_mode IS NULL OR privacy_mode != ?', User::PrivacyPrivate)
               .limit(users_private.count + 1)
               .reverse_order
               .autocompact
               .to_a
    assert_equalities users_public.reverse, results,
                      users_public.size,    s.redis_obj.size
  end

  test_varying 'autocompact deletes inverse',
               klass: [ ListSeries, SortedSetSeries ],
               another_klass: [ ListSeries, SortedSetSeries ] do |var|

    Liker.define_series series_class: var.klass,
      name:       :liked_entries, 
      relation:   ->(u) { Likeable.where(
        test_users: { privacy_mode: Liker::PrivacyPublic }).includes(:user) },
      inverse_of: :likers
    Likeable.define_series series_class: var.another_klass,
      name:       :likers, 
      relation:   Liker, 
      inverse_of: :liked_entries

    # Prepare fixtures
    owners  = 3.times.map{ Liker.create(privacy_mode: Liker::PrivacyPublic) }
    entries = owners.map{ |owner| Likeable.create(user: owner) }
    liker   = Liker.create
    liker.liked_entries.push *entries
    
    # Set an owner to private so that it will be filtered out by Series when autocompact is set
    owners[0].update_attribute :privacy_mode, Liker::PrivacyPrivate
    private_entry = Likeable.find(owners[0].feed_entries[0].id)
    liked_entries = liker.liked_entries.autocompact.to_a

    # Check that the entry by the private owner is indeed purged
    refute liked_entries.include?(private_entry)
    refute private_entry.likers.include?(liker)
  end


  # -------------------------------------------------------------------------
  # Custom assertions

  private
  def assert_array_contains? bigger, smaller
    assert bigger.each_cons(smaller.size).include?(smaller), 
      "#{bigger.map{|i| i.try :id }} does not contain #{smaller.map{|i| i.try :id }}"
  end
  def assert_include? array, item
    assert array.include?(item), 
           "#{array.map{|i| i && i.try(:id) || i }} does not include #{item && item.try(:id) || item }"
  end


  # -------------------------------------------------------------------------
  # Decorator

  class DressedFeedEntry < Draper::Decorator
    delegate_all
    cattr_accessor :mult

    attr_accessor  :key_mult
    def key_mult
      @key_mult ||= key * self.class.mult
    end
  end

  test_varying 'decorator mult', klass: [ SortedSetSeries ] do |var|

    series = var.klass.new name: :test_decorator, relation: FeedEntry, 
                           decorator: DressedFeedEntry, attr_score: :key_mult
    entries = 10.times.map{ FeedEntry.create }

    # Insert users with a temporary multiplier
    DressedFeedEntry.mult = -1000
    series.push entries
    DressedFeedEntry.mult = 0

    # Check results
    results = series.with_scores.to_a
    assert_equalities entries.sort_by{|e| e.key * -1000 },             
                        results,
                      entries.map{|e| e.key * -1000 }.sort, 
                        results.map(&:key_mult)

  end

  test 'decorator with nil' do
    series = SortedSetSeries.new name: :test_decorator,
                                 relation: FeedEntry,
                                 decorator: DressedFeedEntry,
                                 attr_score: :key_mult
    entries = 3.times.map{ |i|
                DressedFeedEntry.new(FeedEntry.create).tap{|d| d.key_mult = i}
              }
    series.push entries

    entries[1].destroy

    results = series.with_scores.to_a
    assert_equalities [entries[0], nil, entries[2]], results,
                      entries[0].key_mult,              results[0].key_mult,
                      entries[2].key_mult,              results[2].key_mult
  end

  # -------------------------------------------------------------------------
  # Sequence number

  test_varying 'sequence number', klass: [ ListSeries, SortedSetSeries, SetSeries ],
                                  preexist: [true, false] do |var|
    series = var.klass.new name: :test_seqnum,
                           relation: FeedEntry,
                           seqnum: true

    # Reading the sequence number initializes it, so we need to test both cases
    assert_equal 0, series.seqnum if var.preexist

    # Add entries
    entries = 6.times.map{ |i|
                FeedEntry.create }
    series.push entries[0,4]
    assert_equal 4, series.seqnum if var.preexist
    series.push entries[4,2]
    assert_equal 6, series.seqnum

    # Delete one
    entries.delete_at(4).destroy
    assert_equal entries, series.autocompact.to_a

    assert_equal 6, series.seqnum

    # Add another one
    series << FeedEntry.create
    assert_equal 7, series.seqnum

  end

  # -------------------------------------------------------------------------
  # Extension methods

  test 'extension methods' do
    u = User.create
    assert_equal 'EXTENDED_LIST', u.extended_list.name_upcase.to_s
  end

  # -------------------------------------------------------------------------
  # Range

  test_varying 'min max offset limit autocompact', min:[200,300], max:[600,800], offset:[nil,1,2], limit:[nil,3,4], autocompact:[true,false] do |var|

    # Create and delete some of the entries
    entries = 10.times.map{|i|
      FeedEntry.create key: i * 100
    }
    s = SortedSetSeries.new name: 'range_test', relation: FeedEntry, attr_score: :key
    s.push *entries.reverse

    # ---- Adjust (WARNING: order is important here!)
    if var.min
      entries.select!{|e| e.key >= var.min }
      s = s.min(var.min)
    end
    if var.max
      entries.select!{|e| e.key <= var.max }
      s = s.max(var.max)
    end
    [300,500,800].each do |key|
      if i = entries.find_index{|e| e && e.key == key }
        entries[i].destroy
        entries[i] = nil
      end
    end
    if var.offset
      entries = entries[var.offset..-1]
      s = s.offset(var.offset)
    end
    if var.autocompact
      entries.compact!
      s = s.autocompact
    end
    if var.limit
      entries = entries[0, var.limit]
      s = s.limit(var.limit)
    end

    assert_equal entries.map{|i| i.try :key },
                 s      .map{|i| i.try :key }

  end

  # -------------------------------------------------------------------------
  # Sample
  
  test 'sample' do
    # ---- First series accepts all Users. 
    series1 = ListSeries.new name: 'sample_test_s1', key: 'sample_test_s1',
                             container: RelationContainer.new,
                             relation: User
                
    u1 = User.create privacy_mode: User::PrivacyPublic
    series1 << u1
    3.times { assert_equal u1, series1.sample.first }
    
    u2 = User.create privacy_mode: User::PrivacyPrivate
    series1 << u2
    5.times { assert [ u1, u2 ].include? series1.sample.first }
    2.times { assert_equal [u1,u2], series1.sample(2).sort_by(&:id) }
    2.times { assert_equal [u1,u2], series1.sample(3).sort_by(&:id) }
    
    # ---- Second series accept only public Users.
    series2 = ListSeries.new name: 'sample_test_s2', key: 'sample_test_s2',
                container: RelationContainer.new,
                relation: User.where(privacy_mode: User::PrivacyPublic)
                
    series2.push u1, u2
    3.times { assert [ u1, nil ].include? series2.sample.first }
    5.times { assert [[u1] , []].include? series2.sample(2) }
    5.times { assert [[u1] , []].include? series2.sample(3) }
  end


end
