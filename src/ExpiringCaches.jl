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
A value is evicted by a timer after `timeout`, without requiring access to
the cache. A timer is created for each stored value. Its callback removes
that value only if it is still current, so replacing a value starts a new
expiration period. Callback execution may be delayed by other tasks.
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

By default, `ExpireOnAccess` keeps expired keys until they are requested
via `haskey` or `get`. Pass `ExpireOnTimeout(timeout)` as the strategy to
remove expired values through timer callbacks without accessing the cache.
"""
struct Cache{K, V, S <: AbstractStrategy} <: AbstractDict{K, V}
    lock::ReentrantLock
    cache::Dict{K, TimestampedValue{V}}
    strategy::S
end
Cache{K, V}(strategy::S = ExpireOnAccess(Dates.Minute(1))) where {K, V, S <: AbstractStrategy} = Cache(ReentrantLock(), Dict{K, TimestampedValue{V}}(), strategy)
Cache{K, V}(timeout::Dates.Period) where {K, V} = Cache{K,V}(ExpireOnAccess(timeout))

expire!(val, key, cache::Cache) = expire!(val, key, cache.strategy)

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
    unlock(x.lock)
    return (state[1][1], state[1][2].value), state[2]
end

function Base.iterate(x::Cache, st)
    lock(x.lock)
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
    unlock(x.lock)
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
        expire!(val_ts, key, cache)
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

"""
    get!(f::Function, cache::ExpiringCaches.Cache, key)

Return the unexpired value for `key`, or compute and cache `f()` outside the
cache lock. Other keys remain accessible while `f()` runs. Concurrent callers
may compute the same key more than once; if another caller stores an unexpired
value before `f()` finishes, return that value instead of overwriting it.
"""
function Base.get!(f::Function, cache::Cache{K, V}, key::K) where {K, V}
    val = lock(cache.lock) do
        if haskey(cache.cache, key)
            x = cache.cache[key]
            if !expired(x, cache.strategy)
                return x
            end
        end
        return nothing
    end
    val !== nothing && return val.value
    computed = f()::V
    return lock(cache.lock) do
        if haskey(cache.cache, key)
            x = cache.cache[key]
            if !expired(x, cache.strategy)
                return x.value
            end
        end
        setindex!(cache, computed, key)
    end
end

Base.delete!(cache::Cache{K}, key::K) where {K} = lock(() -> delete!(cache.cache, key), cache.lock)
Base.empty!(cache::Cache) = lock(() -> empty!(cache.cache), cache.lock)
Base.length(cache::Cache) = length(cache.cache)

"""
    @cacheable strategy function_definition::ReturnType

Cache the results of a named function using eviction `strategy`. Both full and
short-form definitions support named positional arguments, with or without type
annotations and default values. Cache keys contain the actual argument values,
so an omitted default and the same explicit value share an entry.

The function must declare `ReturnType`, which is also the cache's value type.
Keyword arguments, variadic arguments, and `where` parameters are not supported.
"""
macro cacheable(strategy, func)
    func isa Expr && func.head in (:function, :(=)) || throw(ArgumentError("@cacheable expects a function definition"))
    func.args[1] isa Expr && func.args[1].head == :(::) || throw(ArgumentError("@cacheable function must specify return type: $func"))
    returnType = func.args[1].args[2]
    sig = func.args[1].args[1]
    sig isa Expr && sig.head == :call || throw(ArgumentError("@cacheable requires a named function with positional arguments"))
    functionBody = func.args[2]
    funcName = sig.args[1]
    internalFuncName = gensym(:cacheable)
    funcArgs = sig.args[2:end]
    args = map(funcArgs) do arg
        arg = arg isa Expr && arg.head == :kw ? arg.args[1] : arg
        if arg isa Symbol
            return (arg, :Any)
        elseif arg isa Expr && arg.head == :(::) && length(arg.args) == 2 && arg.args[1] isa Symbol
            return (arg.args[1], arg.args[2])
        end
        throw(ArgumentError("@cacheable supports named positional arguments only"))
    end
    argNames = first.(args)
    argTypes = last.(args)
    internalSig = Expr(:(::), Expr(:call, internalFuncName, funcArgs...), returnType)
    internalFunction = Expr(:function, internalSig, functionBody)
    cacheName = gensym()
    return esc(quote
        const $cacheName = ExpiringCaches.Cache{Tuple{$(argTypes...)}, $returnType}($strategy)
        $internalFunction
        Base.@__doc__ function $funcName($(funcArgs...))::$returnType
            return get!($cacheName, tuple($(argNames...))) do
                $internalFuncName($(argNames...))
            end
        end
        ExpiringCaches.getcache(f::typeof($funcName)) = $cacheName
        $funcName
    end)
end

function getcache end

function expire!(val::TimestampedValue{V}, key::K,
                 cache::Cache{K,V,<:ExpireOnTimeout}) where {K, V}
    Timer(Dates.toms(cache.strategy.timeout) / 1000) do _
        lock(cache.lock) do
            if get(cache.cache, key, nothing) === val
                delete!(cache.cache, key)
            end
        end
    end
end

using PrecompileTools

@compile_workload begin
    cache = ExpiringCaches.Cache{Int, Int}(ExpireOnAccess(Dates.Second(5)))
    @assert length(cache) == 0
    @assert isempty(cache)

    @assert get(cache, 1, 2) == 2
    @assert isempty(cache)

    @assert get!(cache, 1, 2) == 2
    @assert !isempty(cache)

    for (k, v) in cache
        @assert v == 2
    end
end

end # module
