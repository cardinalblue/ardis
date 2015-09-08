$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

require 'active_support'
require 'ardis'

# ---------------------------------------------------------------
require 'minitest/autorun'
require 'rr'

# ---------------------------------------------------------------
ActiveSupport.test_order = :random

# ---------------------------------------------------------------
require_relative 'test_varying'
class ActiveSupport::TestCase
  include TestVarying

  # -------------------------------------------------------------------
  def assert_equalities *args
    raise "uneven number of arguments (#{args.count}" unless args.count.even?
    args.each_slice(2).with_index do |(a, b), i|
      assert_equal a, b, "[#{i}]: expected: #{a.inspect}, actual: #{b.inspect}"
    end
  end

  def assert_not_updated evaluatable, &block
    orig = eval evaluatable, block.binding
    yield
    updated = eval evaluatable, block.binding
    assert_equal orig, updated
  end

  def assert_updated evaluatables, &block
    evaluatables = Array(evaluatables)

    origs   = evaluatables.map{ |e| eval e, block.binding }
    yield
    updated = evaluatables.map{ |e| eval e, block.binding }

    origs.zip(updated).each{ |orig, updated|
      assert_not_equal orig, updated
    }
  end

  def assert_equal_sets a, b, *args
    assert_equal Set.new(a), Set.new(b), *args
  end

  def assert_subset bigger, smaller, *args
    bigger  = Set.new bigger
    smaller = Set.new smaller
    assert smaller.subset?(bigger), *args
  end

  # ---------------------------------------------------------
  # Count SQL queries
  # See http://stackoverflow.com/questions/5490411/counting-the-number-of-queries-performed,
  # and http://api.rubyonrails.org/classes/ActiveSupport/Notifications.html
  #
  def db_count_queries &block
    count = 0

    counter_f = ->(name, started, finished, unique_id, payload) {
      unless %w[ CACHE SCHEMA ].include? payload[:name]
        count += 1
      end
    }
    ActiveSupport::Notifications.subscribed(counter_f, "sql.active_record", &block)
    count
  end

  def assert_db_queries count, &block
    c = db_count_queries &block
    assert_equal count, c
  end

end

# ---------------------------------------------------------------
require 'active_record'
ActiveRecord::Base.establish_connection adapter: 'sqlite3',
                                        database: ':memory:'
