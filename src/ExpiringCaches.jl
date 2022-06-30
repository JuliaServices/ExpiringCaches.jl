module ExpiringCaches

using Dates

export Cache, @cacheable, ExpireOnAccess, ExpireOnTimeout

struct TimestampedValue{T}
    value::T
    timestamp::DateTime
end

TimestampedValue(x::T) where {T} = TimestampedValue{T}(x, Dates.now(Dates.UTC))
TimestampedValue{T}(x) where {T} = TimestampedValue{T}(x, Dates.now(Dates.UTC))
timestamp(x::TimestampedValue) = x.timestamp
expired(x::TimestampedValue, timeout) =  (Dates.now(Dates.UTC) - x.timestamp) > timeout

"""
Abstract type for cache eviction policy
"""
abstract type AbstractStrategy end

"""
Evaluate expiration of value `x` given the strategy `s`.
"""
function expired end

"""
Set trigger for expiration of the (`key`,`val`) pair given the strategy.
"""
function expire!(val::V, key::K, s::S) where {K, V, S <: AbstractStrategy} end

"""
A value is evicted if it was present in cache longer then `timeout`.
The eviction occurs during the access to the cached value.
Expired keys will remain in the cache until requested (via
`haskey` or `get`).
"""
struct ExpireOnAccess{P <: Dates.Period} <: AbstractStrategy
    timeout::P
end
expired(x::TimestampedValue, s::ExpireOnAccess) = expired(x, s.timeout)

"""
A value is evicted if it was present in cache longer then `timeout`.
The eviction occurs immediately after expiration timeout.
An async task will be spawned for each key upon entry. When the
timeout task has waited `timeout` length of time, the key will be removed
from the cache.
"""
struct ExpireOnTimeout{P <: Dates.Period} <: AbstractStrategy
    timeout::P
end
expired(x::TimestampedValue, s::ExpireOnTimeout) = expired(x, s.timeout)

"""
    ExpiringCaches.Cache{K, V}(strategy::AbstractStrategy)

Create a thread-safe, expiring cache where value eviction  is determined by
`strategy`.

An `ExpiringCaches.Cache` is an `AbstractDict` and tries to emulate a regular
`Dict` in all respects. It is most useful when the cost of retrieving or
calculating a value is expensive and is able to be "cached" for a certain
amount of time. To avoid using the cache (i.e. to invalidate the cache),
a `Cache` supports the `delete!` and `empty!` methods to remove values
manually.

By default, expired keys will remain in the cache until requested (via
`haskey` or `get`); if `purge_on_timeout=true` keyword argument is passed,
then an async task will be spawned for each key upon entry. When the
timeout task has waited `timeout` length of time, the key will be removed
from the cache.
"""
struct Cache{K, V, S <: AbstractStrategy} <: AbstractDict{K, V}
    lock::ReentrantLock
    cache::Dict{K, TimestampedValue{V}}
    strategy::S
end
Cache{K, V}(strategy::S = ExpireOnAccess(Dates.Minute(1))) where {K, V, S <: AbstractStrategy} = Cache(ReentrantLock(), Dict{K, TimestampedValue{V}}(), strategy)
Cache{K, V}(timeout::Dates.Period) where {K, V} = Cache{K,V}(ExpireOnAccess(timeout))

function Base.iterate(x::Cache)
    lock(x.lock)
    state = iterate(x.cache)
    if state === nothing
        unlock(x.lock)
        return nothing
    end
    while expired(state[1][2], x.strategy)
        state = iterate(x.cache, state[2])
        if state === nothing
            unlock(x.lock)
            return nothing
        end
    end
    return (state[1][1], state[1][2].value), state[2]
end

function Base.iterate(x::Cache, st)
    state = iterate(x.cache, st)
    if state === nothing
        unlock(x.lock)
        return nothing
    end
    while expired(state[1][2], x.strategy)
        state = iterate(x.cache, state[2])
        if state === nothing
            unlock(x.lock)
            return nothing
        end
    end
    return (state[1][1], state[1][2].value), state[2]
end

function Base.haskey(cache::Cache{K, V}, k::K) where {K, V}
    lock(cache.lock) do
        if haskey(cache.cache, k)
            x = cache.cache[k]
            if !expired(x, cache.strategy)
                return true
            else
                delete!(cache.cache, k)
                return false
            end
        end
        return false
    end
end

function Base.setindex!(cache::Cache{K, V}, val::V, key::K) where {K, V}
    lock(cache.lock) do
        val_ts = TimestampedValue{V}(val)
        cache.cache[key] = val_ts
        expire!(val_ts, key, cache.strategy)
        return val
    end
end

function Base.get(cache::Cache{K, V}, key::K, default::V) where {K, V}
    lock(cache.lock) do
        if haskey(cache.cache, key)
            x = cache.cache[key]
            if expired(x, cache.strategy)
                delete!(cache.cache, key)
                return default
            else
                return x.value
            end
        else
            return default
        end
    end
end

function Base.get!(cache::Cache{K, V}, key::K, default::V) where {K, V}
    lock(cache.lock) do
        if haskey(cache.cache, key)
            x = cache.cache[key]
            if expired(x, cache.strategy)
                return setindex!(cache, default, key)
            else
                return x.value
            end
        else
            return setindex!(cache, default, key)
        end
    end
end

function Base.get!(f::Function, cache::Cache{K, V}, key::K) where {K, V}
    lock(cache.lock) do
        if haskey(cache.cache, key)
            x = cache.cache[key]
            if expired(x, cache.strategy)
                return setindex!(cache, f()::V, key)
            else
                return x.value
            end
        else
            return setindex!(cache, f()::V, key)
        end
    end
end

Base.delete!(cache::Cache{K}, key::K) where {K} = lock(() -> delete!(cache.cache, key), cache.lock)
Base.empty!(cache::Cache) = lock(() -> empty!(cache.cache), cache.lock)
Base.length(cache::Cache) = length(cache.cache)

"""
    @cacheable strategy function_definition::ReturnType

For a function definition (`function_definition`, either short-form
or full), create an `ExpiringCaches.Cache` and use eviction `strategy`
(hashed by the exact input arguments obviously).

Note that the function definition _MUST_ include the `ReturnType` declartion
as this is used as the value (`V`) type in the `Cache`.
"""
macro cacheable(strategy, func)
    @assert func.head == :function
    func.args[1].head == :(::) || throw(ArgumentError("@cacheable function must specify return type: $func"))
    returnType = func.args[1].args[2]
    sig = func.args[1].args[1]
    functionBody = func.args[2]
    funcName = sig.args[1]
    internalFuncName = Symbol("__$funcName")
    sig.args[1] = internalFuncName
    funcArgs = sig.args[2:end]
    argTypes = map(x->x.args[2], funcArgs)
    internalFunction = Expr(:function, sig, functionBody)
    cacheName = gensym()
    return esc(quote
        const $cacheName = ExpiringCaches.Cache{Tuple{$(argTypes...)}, $returnType}($strategy)
        $internalFunction
        Base.@__doc__ function $funcName($(funcArgs...))::$returnType
            return get!($cacheName, tuple($(funcArgs...))) do
                $internalFuncName($(funcArgs...))
            end
        end
        ExpiringCaches.getcache(f::typeof($funcName)) = $cacheName
        $funcName
    end)
end

function getcache end

# @cacheable Dates.Minute(2) function foo(arg1::Int, arg2::String)::ReturnType
#     x = x + 1
#     y = y * 2
#     return foobar
# end
# @cacheable Dates.Minute(2) foo(arg1::Int, arg2::Int)::String = # ...

# const CACHE_foo_Int_String = Cache{Tuple{Int, String}, ReturnType}(timeout)
# function foo(args...)::ReturnType
#     return get!(CACHE_foo_Int_String, args) do
#         _foo(args...)
#     en.d
# end
# function _foo(arg1::Int, arg2::String)::ReturnType
#     # ...
# end

function expire!(val::TimestampedValue{V}, key::K,
                 cache::Cache{K,V,ExpireOnTimeout}) where {K, V}
    ts = timestamp(val)
    Timer(div(Dates.toms(s.timeout), 1000)) do _
        lock(cache.lock) do
            val2 = get(cache.cache, key, nothing)
            # only delete if timestamp of original key matches
            if val2 !== nothing && timestamp(val2) == ts
                delete!(cache.cache, key)
            end
        end
    end
end

end # module
