using Test, Dates, ExpiringCaches
using ExpiringCaches: @cacheable

"""
    foo(arg1::Int, arg2::String)

Some docs to check that it doesn't break.
"""
@cacheable ExpireOnAccess(Dates.Second(3)) function foo(arg1::Int, arg2::String)::Float64
    sleep(2)
    return arg1 / length(arg2)
end

@cacheable Dates.Second(3) function foo2(arg1::Int, arg2::String)::Float64
    return arg1 / length(arg2)
end

struct RecordingStrategy <: ExpiringCaches.AbstractStrategy
    keys::Vector{Int}
end
ExpiringCaches.expired(::ExpiringCaches.TimestampedValue, ::RecordingStrategy) = false
ExpiringCaches.expire!(val, key, strategy::RecordingStrategy) = push!(strategy.keys, key)

@testset "ExpiringCaches" begin

cache = ExpiringCaches.Cache{Int, Int}(ExpireOnAccess(Dates.Second(5)))
@test length(cache) == 0
@test isempty(cache)

@test get(cache, 1, 2) == 2
@test isempty(cache)

@test get!(cache, 1, 2) == 2
@test !isempty(cache)

for (k, v) in cache
    @test v == 2
end

sleep(5)

# test that key isn't used after it expires
@test get(cache, 1, 3) == 3

@test get!(()->4, cache, 1) == 4

@test isempty(delete!(cache, 1))
cache[1] = 5
@test !isempty(cache)
@test isempty(empty!(cache))

@testset "timeout evicts stored values" begin
    cache = Cache{Int, Int}(ExpireOnTimeout(Dates.Millisecond(800)))
    cache[1] = 2
    sleep(0.05)
    @test length(cache) == 1
    @test timedwait(() -> length(cache) == 0, 5.0) == :ok
    @test isempty(cache)
end

@testset "custom strategy expiration hook" begin
    strategy = RecordingStrategy(Int[])
    cache = Cache{Int, Int}(strategy)
    cache[1] = 2
    @test strategy.keys == [1]
    @test get(cache, 1, 0) == 2
end

@testset "timeout preserves a different entry with the same timestamp" begin
    cache = Cache{Int, Int}(ExpireOnTimeout(Dates.Millisecond(50)))
    cache[1] = 1
    previous = cache.cache[1]
    replacement = ExpiringCaches.TimestampedValue{Int}(2, previous.timestamp)
    cache.cache[1] = replacement
    sleep(0.15)
    @test get(cache.cache, 1, nothing) === replacement
end

@testset "get! cached nothing" begin
    for V in (Nothing, Union{Nothing, Int})
        cache = Cache{Int, V}(Dates.Minute(1))
        cache[1] = nothing
        calls = Ref(0)
        @test get!(() -> (calls[] += 1; nothing), cache, 1) === nothing
        @test calls[] == 0
    end
end

@testset "get! permits access to another key" begin
    cache = Cache{Int, Int}(Dates.Minute(1))
    started = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    first = @async get!(cache, 1) do
        put!(started, nothing)
        take!(release)
        1
    end
    take!(started)
    second = @async get!(() -> 2, cache, 2)
    status = timedwait(() -> istaskdone(second), 5.0)
    put!(release, nothing)
    @test status == :ok
    @test fetch(first) == 1
    @test fetch(second) == 2
end

@testset "get! preserves a newer value" begin
    for newer in (nothing, 99)
        cache = Cache{Int, Union{Nothing, Int}}(Dates.Minute(1))
        started = Channel{Nothing}(1)
        release = Channel{Nothing}(1)
        pending = @async get!(cache, 1) do
            put!(started, nothing)
            take!(release)
            5
        end
        take!(started)
        writer = @async setindex!(cache, newer, 1)
        status = timedwait(() -> istaskdone(writer), 5.0)
        put!(release, nothing)
        @test status == :ok
        fetch(writer)
        @test fetch(pending) === newer
        @test get(cache, 1, nothing) === newer
    end
end

@test foo(1, "ffff") == 0.25
tm = @elapsed foo(1, "ffff")
@test tm < 2 # test that normal function body wasn't executed
sleep(3)
tm = @elapsed foo(1, "ffff")
@test tm > 2 # test that normal function body was executed

cache = ExpiringCaches.Cache{Int, Int}(ExpireOnTimeout(Dates.Second(5)))
cache[1] = 2
@test !isempty(cache)
sleep(3)
cache[1] = 3
sleep(2.5)
# key isn't purged because we replaced it, so timer is "reset"
@test !isempty(cache)
sleep(3)
# key is now purged w/o being accessed
@test length(cache) == 0
@test isempty(cache)

end
