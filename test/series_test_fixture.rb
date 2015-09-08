require 'active_record'

module Ardis
module SeriesTestFixture

class FeedEntry < ActiveRecord::Base
  include Ardis

  attr_accessor :my_accessor

  self.table_name = 'test_feed_entries'

  after_initialize do
    self.key ||= rand(1_000_000_000_000)
    self.created_at ||= rand(100).days.ago
  end


  belongs_to :user
  has_one    :feed_entry_detail

  series_reference :user, :referenced_list

  # ----------------------------------------
  if (Kaminari rescue nil)
    include Kaminari::ConfigurationMethods
    paginates_per 9  # Kaminari configuration, some odd number
  end
  

end

class User < ActiveRecord::Base
  include Ardis

  self.table_name = 'test_users'

  PrivacyPublic  = 0
  PrivacyPrivate = 1

  has_many :feed_entries

  series_list       name: :my_list, relation: FeedEntry,
                    key:  ->(user){ "list>#{user.id}" },
                    initializer: lambda{|user| user.send :my_initializer },
                    redis_opts: { maxlength: 128 }

  series_sorted_set name: :my_sorted_set,     relation: FeedEntry,
                    key:  ->(user){ "sorted_set>#{user.id}" },
                    initializer: :my_initializer,
                    attr_score: :key
  series_set        name: :my_set,            relation: FeedEntry,
                    key:  ->(user){ "set>#{user.id}" },
                    initializer: :my_initializer

  series_list       name: :my_global_list, global: true, relation: FeedEntry,
                    key:  :my_global_list_key
  series_sorted_set name: :my_global_sorted_set, global: true, relation: FeedEntry,
                    key:  :my_global_sorted_set_key
  series_set        name: :my_global_set, global: true, relation: FeedEntry,
                    key:  :my_global_set_key

  series_list       name: :referenced_list, relation: FeedEntry

  series_list       name: :extended_list, relation: FeedEntry do
    def name_upcase
      name.upcase
    end
    def container_class
      container.class
    end
  end


  def my_initializer
    10.times.collect{ FeedEntry.create(user: self) }
  end


end

class FeedEntryDetail < ActiveRecord::Base
  self.table_name = 'test_feed_entry_details'

  belongs_to :feed_entry
end

# Migrations
class Migration < ActiveRecord::Migration
  def self.up
    drop_table   :test_users rescue nil
    create_table :test_users do |t|
      t.string     :name
      t.integer    :privacy_mode
      t.timestamps null: true
    end

    drop_table   :test_feed_entries rescue nil
    create_table :test_feed_entries do |t|
      t.integer    :key, limit: 8
      t.references :user
      t.timestamps null: true
    end

    drop_table   :test_feed_entry_details rescue nil
    create_table :test_feed_entry_details do |t|
      t.references :feed_entry
      t.timestamps null: true
    end
  end
end

# Class used to just test the BaseSeries superclass.
# Hides an Array behind a Series facade.
#
class ArraySeries < Ardis::BaseSeries
  attr_accessor :array

  def initialize(array: [],
                 **options,
                 &extension_block)
    super options, &extension_block
    self.array = array
    @updated_at = Time.now
  end

  # ---------------------------------------------------------------
  # Required overrides

  def total_count
    self.array.count
  end

  # -------
  def insert_ids ids, prepend: false
    ids = ids.map &:to_i
    if prepend
      self.array = ids.reverse + self.array
    else
      self.array += ids
    end
    ids.count
  end

  def fetch_ids offset=0, limit=nil, reverse: false
    a = reverse ? array.reverse : array
    ((limit ? a[offset, limit] : a[offset..-1]) || []).map &:to_i
  end

  def delete_ids ids
    ids = ids.map &:to_i
    ids.map{|id|
      self.array.delete id
    }.compact.count
  end

  # --------
  def update_updated_at updated_count: nil
    @@updated_at ||= {}
    @@updated_at[name] = Time.now
  end
  def read_updated_at
    @@updated_at ||= {}
    @@updated_at[name] || Time.at(0)
  end

  # --------
  def include? obj
    super(obj) and begin
      id = ensure_id(obj)
      resolve_ids.include? id.to_i
    end
  end

  # --------
  def del
    self.array = []
  end

end

end
end

