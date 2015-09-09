# Ardis

Ardis is a simple library that allows you to leverage the NoSQL [Redis](http://redis.io/) store to index, group, rank or associate traditional `ActiveRecord` objects.
Internally, Ardis *only* stores the `id`s of the `ActiveRecord` objects in Redis datastructures, but provides an `ActivRecord` association-like interface.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'ardis'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install ardis

## Usage

Basic usage is using the DSL to create a class-level collection (i.e. a "Series").

```ruby
require 'ardis'

class MyModel < ActiveRecord::Base
  include Ardis
  series_list name: 'a_collection', global: true
```

    irb> m = MyModel.create
     => #<MyModel id: 1, ... >

    irb> MyModel.a_collection << m
     => #<Ardis::RedisAdapter::ListSeries @name=:a_collection ... >

    irb> MyModel.a_collection.first
     => #<MyModel id: 1, ... >

Or you can create an instance-level Series:

```ruby
class MyModel < ActiveRecord::Base
  ...
  series_list name: 'users', relation: User
```

    irb> m = MyModel.create
     => #<MyModel id: 1, ... >

    irb> m.users << User.create(name: 'John Doe')
     => #<Ardis::RedisAdapter::ListSeries @name=:users ... >

    irb> m.users << User.create(name: 'Jane Tow')
     => #<Ardis::RedisAdapter::ListSeries @name=:users ... >

    irb> m.users.limit(2).to_a
     => [ #<User id: 1, ... >, #<User id: 2, ... > ]

The instance that holds the Series (in the example above `m`) is referred to as the "container".

When retrieveing objects from a collection, most of the methods familiar from
`ActiveRecord` relations work:

```ruby
m.users.limit(10).offset(2).reverse_order
m.users.page(2)                      # `Kaminari` integration
m.users.includes?(@joe)              # Check if in collection
m.users.includes(:some_association)  # Eager-loading
m.users.count                        # Can be faster, unlike PostgreSQL
m.users.empty?
m.users.delete(@joe)
```

#### Redis keys
If using the DSL, Ardis will choose the appropriate Redis keys based on the Class
name, container and given name, and depending if the Series is `global` or not, but the key
can always be overridden manually:

```ruby
series_sorted_set name: 'my_collection', key: 'custom:redis:key', global: true
```

#### Series types
Implementations are provided for the basic [Redis datastructures](http://redis.io/topics/data-types):
- [Lists](http://redis.io/topics/data-types-intro#lists)
- [Sets](http://redis.io/topics/data-types-intro#sets)
- [Sorted sets](http://redis.io/topics/data-types-intro#sorted-sets)

#### Inverse Series
Two Series' can be declared inverses of each other in a reciprocal association, so that
inserting into one, automatically inserts into the other one.

For example, suppose we have `User`s that like `Photo`s.

```ruby
class User < ActiveRecord::Base
  series_list name: 'liked_photos', relation: Photo, inverse_of: 'likers'
end

class Photo < ActiveRecord::Base
  series_list name: 'likers', relation: User, inverse_of: 'liked_photos'
end
```

Internally they are two different Redis lists, but you only have to insert and remove from one of them.

#### Autocompact
If the underlying `ActiveRecord` row is deleted, Ardis will return `nil` for that object.
Running a query with `autocompact` automatically purges that id from the Redis datastructure.

```ruby
irb> s = Ardis::RedisAdapter::ListSeries.new name: 'list', relation: User

irb> s << User.create name: 'Bill'

irb> s << User.create name: 'Clay'

irb> s.to_a
 => [ #<User name: 'Bill'>, #<User name: 'Clay'> ]

irb> User.where(name: 'Bill').destroy_all

irb> s.limit(2).to_a
 => [ nil, #<User name: 'Clay'> ]

irb> s.limit(2).autocompact.to_a
 => [ #<User name: 'Clay'> ]

irb> s.limit(2).to_a
 => [ #<User name: 'Clay'> ]
```

#### Advanced usage
(Documentation coming soon)
- `initializer`

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/cardinalblue/ardis.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

