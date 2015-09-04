require 'redis'
require 'draper'

# -----------------------------------------------------
# Patches redis-object

class Redis::Set

  # Patch Redis::Set to support (in a half-assed manner) the same methods
  # as a List.
  #
  def push(*values)
    add(values)
  end
  def unshift(value)
    add(value)
  end

end

module Ardis
module Redis

class RedisSeries < ::Ardis::BaseSeries
  include AttrStrategy

  # PRIVATE accessors
  attr_accessor :key,
                :seqnum,
                :redis_client

  def initialize(key:             nil,
                 seqnum:          false,
                 redis_client:    nil,
                 redis_opts:      {},
                 **options,
                 &extension_block)

    super options, &extension_block

    if !name && !key
      raise ArgumentError.new('name or key must be given')
    end

    @key              = key
    @seqnum           = seqnum
    @redis_opts       = redis_opts

    # Attributes
    @redis_client     = redis_client || ::Redis.current

  end

  # ---- Arel-like cloning interface

  # Used for pipelining
  def with_redis redis_client
    series = clone
    series.instance_variable_set(:@redis_client, redis_client)
    series
  end

  # --------------------------------------------------------
  # Public Series interface overrides

  public
  def del
    redis_obj_uninitialized.del
    @redis_obj = nil  # So that we might trigger the initializer next
  end
  def exists?
    redis_obj_uninitialized.exists?
  end

  # --------------------------------------------------------
  # Note that we should ALWAYS go thru the redis_obj, even if we use
  # the client directly, as this is how the auto-initialize mechanism works

  def redis_obj
    run_initializer?
    redis_obj_uninitialized
  end
  def redis_obj_uninitialized
    @redis_obj ||= actual_redis_obj
  end

  # ---------------------------------------------------------
  # updated_at - subclasses should override

  protected
  def read_updated_at
    v = updated_at_hash[actual_key]
    v && Time.at( v.to_f )
  end
  def update_updated_at updated_count: nil
    if updated_count.nil? || updated_count > 0
      updated_at_hash[actual_key] = Time.now.to_f
    end
  end
  def updated_at_hash
    @updated_at_hash ||= ::Redis::HashKey.new(RedisKey::CB_SERIES_UPDATED_AT)
  end

  # ---------------------------------------------------------
  # Sequence number

  SEQNUM_SCRIPT = <<-LUA
    local key   = KEYS[1]
    local name  = ARGV[1]
    if redis.call('HEXISTS', key, name) == 1 then
      return tonumber(redis.call('HGET', key, name))
    else
      local type = redis.call('TYPE', name)['ok']
      local length
      if     type == 'list' then length = redis.call('LLEN',  name)
      elseif type == 'zset' then length = redis.call('ZCARD', name)
      elseif type == 'set'  then length = redis.call('SCARD', name)
      else return 0
      end
      redis.call('HSET', key, name, length)
      return tonumber(length)
    end
  LUA

  SEQNUM_INCREMENT_SCRIPT = <<-LUA
    local key   = KEYS[1]
    local name  = ARGV[1]
    local delta = ARGV[2]
    if redis.call('HEXISTS', key, name) == 1 then
      redis.call('HINCRBY', key, name, delta)
    end
  LUA

  public
  def seqnum
    raise "seqnum not enabled" unless @seqnum
    redis_obj.redis.eval_and_load_script(
        SEQNUM_SCRIPT, [ RedisKey::CB_SERIES_SEQNUMS ], [ actual_key ]
    )
  end

  protected
  def increment_seqnum delta=1
    redis_obj.redis.eval_and_load_script(
        SEQNUM_INCREMENT_SCRIPT, [ RedisKey::CB_SERIES_SEQNUMS ], [ actual_key, delta ])
  end

  # --------------------------------------------------------
  # User provided initializers should meet these requirements:
  #
  # * In the case of SortedSets, the initializer is restricted to assign
  #   values that are attributes of the relation, since we're using `push` here
  # * In the case where `max_length: len` is set, the initializer should
  #   remember not to return objects that exceed the max_length
  #
  def run_initializer?
    if initializer && !@checked_initializer

      # Because it is faster for the more common case, initially just check the
      # existence, THEN if not found, do the WATCH command, (then again the existence).
      #
      if !redis_obj_uninitialized.exists?

        # Initialize atomically using redis WATCH
        # command to avoid race conditions.
        #
        redis_client.watch actual_key do

          # Make sure that everything we set up the redis_obj
          # without initialization, otherwise we'll infinitely recurse.
          if redis_obj_uninitialized.exists? || (initials = call_initializer).blank?
            redis_client.unwatch
          else
            # Note that if the execution failed because of the WATCH then all
            # the operations inside the push get cancelled, so it's as if nothing
            # happened. Because what we want is to not initialize if somebody else
            # already initialized the key, this is OK.
            # The multi command always issues an EXEC (even if no operations), so
            # that cancels the WATCH
            #
            redis_client.multi do |multi|
              with_redis(multi).push *initials
            end

          end
        end
      end

      @checked_initializer = true
    end
  end

  # --------------------------------------------------------
  # Internal utility

  protected
  def actual_redis_obj
    raise NotImplementedError, "subclass must implement"
  end
  def actual_key
    if key
      key.respond_to?(:call) ? key.call(container) : key
    elsif container
      if container.kind_of? ContainerProxy
        "#{container.klass.name.underscore}:#{container.id}:#{name}"
      elsif container.respond_to? :id
        "#{container.class.name.underscore}:#{container.id}:#{name}"
      elsif Module === container
        "#{container.name.underscore}::#{name}"
      else
        raise ArgumentError, "invalid container #{container.inspect}"
      end
    else
      # No container, use the name
      name
    end
  end



end

class ListSeries < RedisSeries
  include Draper::Decoratable

  # ---------------------------------------------------------
  # Series overrides: Readers
  #
  public
  def total_count
    # As an optimization, check the uninitialized one first, only check on
    # initialized if we get 0.
    if (c = redis_obj_uninitialized.size) > 0
      c
    else
      redis_obj.size
    end
  end
  def include? obj
    super(obj) and begin
      # Check id in Redis
      id = ensure_id(obj).to_s
      if offset_value.presence || limit_value.presence
        # This is not great performance, since it loads all of them,
        # should actually try to check directly in Redis, optimize later.
        resolve_ids.include?(id)
      else
        _includes_id? id
      end
    end

  end

  # ------------------------------------------------------
  # Required id resolution overrides

  def insert_ids ids, prepend: false
    return 0 if ids.empty?
    ret = if prepend
            redis_obj.unshift *ids
          else
            redis_obj.push *ids
          end

    # Check to see if we got a future or a number
    return (if ret.is_a? Numeric
              ret
            else
              ids.count
            end)
  end

  def fetch_ids offset=0, limit=nil, reverse: false
    return [] if limit && limit <= 0  # Otherwise can't really calculate start/stop correctly
    start, stop = _calculate_start_stop(offset, limit, reverse: reverse)
    fetched = redis_obj.redis.lrange(actual_key, start, stop)
    fetched.reverse! if reverse
    fetched
  end

  def delete_ids ids
    ids.map{|id|
      delete_count = redis_obj.delete id.to_s
      Integer(delete_count) rescue 1  # Default value if Redis::Future
    }.inject 0, &:+
  end

  # ------------------------------------------------------
  # Overrides to update the seqnum
  protected
  def _insert objs, prepend: false, inversion_disabled: false
    super(objs, prepend: prepend, inversion_disabled: inversion_disabled)
    increment_seqnum objs.count
  end
  def _delete objs, inversion_disabled: false
    delete_count = super(objs, inversion_disabled: inversion_disabled)
    increment_seqnum -objs.count
    delete_count
  end

  # ------------------------------------------------------
  # Internal resolution
  private

  # Assumes limit.nil? || limit > 0
  #
  def _calculate_start_stop offset=0, limit=nil, reverse:false
    start = offset
    stop  = limit ? [offset + limit - 1, 0].max : -1
    reverse ?  [ -stop - 1, -start - 1 ] :
        [ start, stop ]
  end

  def _includes_id? id
    redis_obj.include? id.to_s
  end

  # ------------------------------------------------------
  # Dependent on underlying Redis object
  protected
  def actual_redis_obj
    ::Redis::List.new(*redis_obj_args)
  end
  def redis_obj_args
    [actual_key, redis_client, @redis_opts].compact
  end

end

# Support Set as half-broken Lists, this requires us to patch the Redis::Set
# to support a similar interface to List (see below).
#
class SetSeries < ListSeries

  # ------------------------------------------------------
  # Overrides dependent on underlying Redis object
  protected

  def actual_redis_obj
    ::Redis::Set.new(*redis_obj_args)
  end
  def delete_ids ids
    delete_count = redis_obj.delete ids if ids.present?
    Integer(delete_count) rescue ids.count  # Default value if Redis::Future
  end

  # Note that index should be ABSOLUTE and not the "negative index" that Redis or Ruby do.
  #
  def fetch_ids offset=0, limit=nil, reverse: false
    members = redis_obj.redis.smembers(actual_key)

    # Ruby Arrays have a different behavior if offset/ranges are out of range,
    # so we don't use `_calculate_start_stop` here.
    members.reverse! if reverse
    members[offset, limit || members.length] || []
  end

end

class SortedSetSeries < ListSeries
  include Draper::Decoratable

  # PRIVATE accessors
  attr_accessor :with_scores_value,
                :min_value,
                :max_value,
                :score_strategy

  # ---- Object lifecycle
  public

  # Parameters:
  #   - attr_score: can have two forms:
  #       1) A symbol or string: refers to an attribute of the relation object.
  #          The score will be read from it and set if there is a setter
  #          (i.e. "#{attr_score}=").
  #       2) A Proc: will get called with the relation object to obtain the score.
  #          If the Proc is arity of -2 (accepts an optional second parameter), it
  #          should behave as a setter and accepts the score value upon retrieving
  #          the object.
  #
  def initialize(attr_score: :id,
                 **options,
                 &extension_block)

    super options, &extension_block

    self.attr_score = attr_score  # This will set the ScoreStrategy

    # ---------------------------------------------------
    # Initialize INTERNAL ivars
    self.with_scores_value = false

    # Result caches. These will be copied to clones, so should
    # be invariant to new queries. Clones *should* get and use
    # the same cache/Hash.
    #
    @reported_scores   = {}
    @calculated_scores = {}

  end

  # ---------------------------------------------------------
  # New public interface methods

  public

  def min(value)
    series = clone
    series.min_value = value
    series
  end
  def max(value)
    series = clone
    series.max_value = value
    series
  end

  # Retrieves the score for the given object.
  #
  def score_for(obj)
    id = ensure_id(obj).to_s
    @reported_scores[id] || redis_obj.score(id)
  end

  # If called on the Series, sets it so that upon retrieval, the scores
  # will also be fetched.
  #
  def with_scores(with_scores=true)
    series = clone
    series.with_scores_value = with_scores
    series
  end

  def index_of obj
    id = ensure_id(obj).to_s
    if reverse_order_value
      redis_obj.revrank(id)
    else
      redis_obj.rank(id)
    end
  end

  # ---------------------------------------------------------
  # Setters/getters

  def attr_score= val
    @attr_score = val

    case val
      when Symbol, String
        self.score_strategy = NamedAttrStrategy.new(val)
      when Proc
        self.score_strategy = ProcAttrStrategy.new(val)
      else
        raise ArgumentError, "invalid #{val.inspect}"
    end
  end


  # ---------------------------------------------------------
  # Insertion (overrides)

  public
  def []=(obj, value)
    redis_obj[ensure_id(obj)] = value
    update_updated_at updated_count: 1
    increment_seqnum if @seqnum
  end
  def [](obj)
    redis_obj[ensure_id(obj)]
  end
  def incr(obj, val=1)
    redis_obj.incr(ensure_id(obj), val)
  end
  def decr(obj, val=1)
    redis_obj.decr(ensure_id(obj), val)
  end

  alias :increment :incr
  alias :decrement :decr

  # ---------------------------------------------------------
  # Override from ListSeries

  def first
    _report_score(super)
  end
  def last
    _report_score(super)
  end
  def to_a
    _report_scores(super)
  end
  
  # ---------------------------------------------------------
  # Unions
  
  def union_with other, weights: [1,1], aggregate: :max, initialize: false
    raise InvalidArgumentError.new('The first argument should be a valid SortedSetSeries!') \
      unless other.kind_of?(self.class)
    raise InvalidArgumentError.new('Both series should use the same container and relation!') \
      unless container == other.container && relation == other.relation

    union = self.class.new name:       "UNION::#{actual_key}::#{other.actual_key}",
                           container:  container,
                           relation:   relation,
                           attr_score: -> { attr_score }
                           
    if !union.exists? || initialize
      redis_client.zunionstore(union.actual_key,
                               [actual_key, other.actual_key],
                               weights:   weights,
                               aggregate: aggregate)
      # The timestamp has to be bumped manually
      union.update_updated_at
    end

    union
  end

  # ---------------------------------------------------------

  protected
  def _insert objs, prepend: false, inversion_disabled: false
    _calculate_scores objs
    super objs, prepend: prepend, inversion_disabled: inversion_disabled
  end

  private
  def _calculate_scores(objs)
    # Extract scores
    @calculated_scores = Hash[objs.map{|obj|
      id = ensure_id(obj).to_s

      # Only apply Decorator if it's an object not already decorated.
      # Currently it only matters if we care about the score.
      #
      if decorator && !obj.kind_of?(decorator) && obj.respond_to?(:id)
        obj = decorator.new(obj)
      end

      # Calculate score
      score = score_strategy.calculate_attr(obj).to_f

      # Map
      [id, score]
    }]
  end
  def _report_score(obj)
    if with_scores_value && obj
      score = @reported_scores[obj.id.to_s] || score_for(obj)
      score_strategy.report_attr(obj, score)
    end
    obj
  end
  def _report_scores(objs)
    return objs if not with_scores_value
    objs.map{|obj|
      _report_score(obj)
    }
  end

  # ------------------------------------------------------
  # Required overrides
  # Dependent on underlying Redis object.

  protected
  def actual_redis_obj
    ::Redis::SortedSet.new(*redis_obj_args)
  end

  def insert_ids ids, prepend: false
    return 0 if ids.empty?
    score_ids = ids.map{|id|
      score = @calculated_scores[id.to_s] || 0
      [ id.to_s, score ]
    }
    ret = redis_obj.merge(score_ids)

    # Check to see if we got a future or a number
    return (if ret.is_a? Numeric
              ret
            else
              ids.count
            end)
  end

  # Note that index should be ABSOLUTE and not the "negative index" that Redis or Ruby do.
  #
  def fetch_ids offset=0, limit=nil, reverse: false
    return [] if limit && limit <= 0  # Otherwise can't really calculate start/stop correctly

    start, stop = _calculate_start_stop offset, limit  # Use Redis reverse

    if min_value || max_value
      cmd, arg1, arg2 = reverse ?
          [ :zrevrangebyscore,  max_value || '+inf', min_value || '-inf' ] :
          [ :zrangebyscore,     min_value || '-inf', max_value || '+inf' ]
      fetched = redis_obj.redis.send(cmd, actual_key,
                                     arg1, arg2,
                                     with_scores: with_scores_value,
                                     limit: [ offset, limit || 1_000_000])
    else
      cmd     = reverse ? :zrevrange : :zrange
      fetched = redis_obj.redis.send(cmd, actual_key,
                                     start, stop,
                                     with_scores: with_scores_value)
    end
    return process_fetched_into_ids(fetched, with_scores: with_scores_value)
  end

  def delete_ids ids
    delete_count = redis_obj.delete ids if ids.present?
    Integer(delete_count) rescue ids.count  # Default value if Redis::Future
  end

  def _includes_id? id
    !!redis_obj.member?(id)
  end

  # --------------------------------------------------------
  # Private utility
  def process_fetched_into_ids(fetched, with_scores:)
    if with_scores
      @reported_scores.merge! Hash[fetched]
      fetched.map &:first
    else
      fetched
    end
  end

end

end
end

