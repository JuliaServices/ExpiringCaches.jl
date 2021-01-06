using Test, Dates, ExpiringCaches

"""
    foo(arg1::Int, arg2::String)

Some docs to check that it doesn't break.
"""
ExpiringCaches.@cacheable Dates.Second(3) function foo(arg1::Int, arg2::String)::Float64
    sleep(2)
    return arg1 / length(arg2)
end

@testset "ExpiringCaches" begin

cache = ExpiringCaches.Cache{Int, Int}(Dates.Second(5))
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

@test foo(1, "ffff") == 0.25
tm = @elapsed foo(1, "ffff")
@test tm < 2 # test that normal function body wasn't executed
sleep(3)
tm = @elapsed foo(1, "ffff")
@test tm > 2 # test that normal function body was executed

end