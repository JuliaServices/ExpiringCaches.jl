module ExpiringCaches

using Dates

export Cache, @cacheable

struct TimestampedValue{T}
    value::T
    timestamp::DateTime
end

TimestampedValue(x::T) where {T} = TimestampedValue{T}(x, Dates.now(Dates.UTC))
TimestampedValue{T}(x) where {T} = TimestampedValue{T}(x, Dates.now(Dates.UTC))

"""
    ExpiringCaches.Cache{K, V}(timeout::Dates.Period; purge_on_timeout::Bool=false)

Create a thread-safe, expiring cache where values older than `timeout`
are "invalid" and will be deleted.

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
struct Cache{K, V, P <: Dates.Period} <: AbstractDict{K, V}
    lock::ReentrantLock
    cache::Dict{K, TimestampedValue{V}}
    timeout::P
    purge_on_timeout::Bool
end
Cache{K, V}(timeout::Dates.Period=Dates.Minute(1); purge_on_timeout::Bool=false) where {K, V} = Cache(ReentrantLock(), Dict{K, TimestampedValue{V}}(), timeout, purge_on_timeout)

expired(x::TimestampedValue, timeout) = (Dates.now(Dates.UTC) - x.timestamp) > timeout

function Base.iterate(x::Cache)
    lock(x.lock)
    state = iterate(x.cache)
    if state === nothing
        unlock(x.lock)
        return nothing
    end
    while expired(state[1][2], x.timeout)
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
    while expired(state[1][2], x.timeout)
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
            if !expired(x, cache.timeout)
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
        cache.cache[key] = TimestampedValue{V}(val)
        if cache.purge_on_timeout
            Timer(div(Dates.toms(cache.timeout), 1000)) do _
                lock(cache.lock) do
                    delete!(cache.cache, key)
                end
            end
        end
        return val
    end
end

function Base.get(cache::Cache{K, V}, key::K, default::V) where {K, V}
    lock(cache.lock) do
        if haskey(cache.cache, key)
            x = cache.cache[key]
            if expired(x, cache.timeout)
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
            if expired(x, cache.timeout)
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
            if expired(x, cache.timeout)
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
    @cacheable timeout function_definition::ReturnType

For a function definition (`function_definition`, either short-form
or full), create an `ExpiringCaches.Cache` and store results for `timeout`
(hashed by the exact input arguments obviously).

Note that the function definition _MUST_ include the `ReturnType` declartion
as this is used as the value (`V`) type in the `Cache`.
"""
macro cacheable(timeout, func)
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
        const $cacheName = ExpiringCaches.Cache{Tuple{$(argTypes...)}, $returnType}($timeout)
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
#     end
# end
# function _foo(arg1::Int, arg2::String)::ReturnType
#     # ...
# end

end # module