require_relative 'test_helper'

require 'ardis/autocompacter'

module Ardis
  class AutocompacterTest < ActiveSupport::TestCase

    test 'simple' do
      a = [ nil, 1, nil, 2, 3, nil, 4, nil, 5 ]
      actual = Autocompacter.autocompact 3, 5 do |offset, limit|
        cur = a[offset, limit]
        cur.present? && cur.compact
      end
      assert_equal [2,3,4,5], actual
    end

    A = [ nil, :a, :b, nil, :c, nil, :d, nil, nil, nil, :e, nil, nil, :f, nil, nil, nil ]

    test_varying 'autocompact',
      deleting: [true, false],
      offset_limit: [
        [0, 2],
        [2, 4],
        [2, 40],
        [A.length - 2, 10],
        [A.length, 10],
        [A.length + 2, 10],
        [10, -10],
        [-10, 10],
        [-10, -10],
        [-20, 10],
        [3, 0],
        [-3, 3],
        [-6, 3],
        [-9, 3],
        [-12, 3],
        [-15, 3],
        [3, -1],
        [-20, 2],
      ] do |var|

      offset, limit = var.offset_limit

      a = A.clone  # Off-range returns nil, but we will return []

      expected = (A[offset..-1] || []).compact[0, limit || 1] || []
      actual   = Autocompacter.autocompact offset, limit, deleting: var.deleting do |offset, limit|
        cur = a[offset, limit] || []
        if var.deleting
          to_delete = select_indices(cur, &:nil?)
          select_indices(cur, &:nil?).reverse.each do |rel_index|
            a.delete_at offset + rel_index
          end
        end
        cur.present? && cur.compact
      end

      assert_equal expected, actual
    end

    test_varying 'uniq', uniq:[true, false] do |var|
      a =  [ nil, :a, :b, nil, :c, :b, :e, nil, :d, nil, nil, :e, nil, :f, :d, nil ]
      actual = Autocompacter.autocompact 2, 5, deleting:false, uniq:var.uniq do |offset, limit|
        cur = a[offset, limit]
        cur.present? && cur.compact
      end
      expected = var.uniq ? [:b, :c, :e, :d, :f] :
                            [:b, :c, :b, :e, :d]
      assert_equal expected, actual
    end

    private
    def select_indices enum, &block
      enum.each_with_index.map{|i, index|
        index if block.call(i)
      }.compact
    end

  end
end