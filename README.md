
# ExpiringCaches.jl

*A Dict type with expiring values and a `@cacheable` macro to cache function results in an expiring cache*

## Installation

The package is registered in the [`General`](https://github.com/JuliaRegistries/General) registry and so can be installed at the REPL with `] add ExpiringCaches`.

## Usage


### `Cache`
    ExpiringCaches.Cache{K, V}(timeout::Dates.Period)
    ExpiringCaches.Cache{K, V}(strategy::ExpiringCaches.AbstractStrategy)

Create a thread-safe, expiring cache where values older than `timeout`
are "invalid" and will be deleted.

An `ExpiringCaches.Cache` is an `AbstractDict` and tries to emulate a regular
`Dict` in all respects. It is most useful when the cost of retrieving or
calculating a value is expensive and is able to be "cached" for a certain
amount of time. To avoid using the cache (i.e. to invalidate the cache),
a `Cache` supports the `delete!` and `empty!` methods to remove values
manually.

The default `ExpiringCaches.ExpireOnAccess(timeout)` strategy removes expired
values when they are requested. Use `ExpiringCaches.ExpireOnTimeout(timeout)`
to remove values through timer callbacks without accessing the cache:

```julia
using ExpiringCaches, Dates

cache = ExpiringCaches.Cache{String, Int}(
    ExpiringCaches.ExpireOnTimeout(Dates.Second(30)),
)
```


### `@cacheable`
    @cacheable strategy function_definition::ReturnType

Cache a function's results using an eviction strategy or timeout. Full and
short-form definitions support named positional arguments with optional type
annotations and default values:

```julia
@cacheable Dates.Minute(1) function add(one, two::Int=2)::Int
    one + two
end

add(1)       # computes and caches 3
add(1, 2)    # returns the same cached value
```

The return type is required and determines the cache's value type. Keyword
arguments, variadic arguments, and `where` parameters are not supported.
