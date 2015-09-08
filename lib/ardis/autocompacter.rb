module Ardis
module Autocompacter

  # Implements an autocompacting algorithm.
  # The required block must receive an offset and limit, and return `nil` once
  # there are no more items that can be read. Otherwise, should return an array
  # of compacted items.
  # Negative offsets are allowed.
  #
  # Usage:
  #   a = [ nil, 1, nil, 2, 3, nil, 4, nil, 5 ]
  #   autocompact 3, 5 do |offset, limit|
  #     cur = a[offset, limit]
  #     cur.present? && cur.compact
  #   end
  #
  # Will return [2, 3, 4, 5]
  #
  def self.autocompact offset, limit, deleting: false, uniq: false
    ret = []
    neg = offset < 0
    limit_orig = limit
    while limit > 0 do
      cur = yield offset, limit
      break if !cur
      ret += cur
      ret.uniq! if uniq
      offset += (deleting && !neg) ? cur.length : limit
      break if neg && offset >= 0
      limit = limit_orig - ret.length
    end
    ret
  end

end
end