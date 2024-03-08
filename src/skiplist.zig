const std = @import("std");

const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const Self = @This();

const K = []const u8;
const V = []const u8;
const ArrayList = std.ArrayListUnmanaged(*Node);
const MAX_LEVEL = 4;

var prng = std.Random.DefaultPrng.init(0);

fn seed(s: u64) void {
    prng.seed(s);
}

allocator: Allocator,
list: [MAX_LEVEL]ArrayList,

pub fn init(allocator: Allocator) Self {
    return .{
        .allocator = allocator,
        .list = [_]ArrayList{.{}} ** MAX_LEVEL,
    };
}

pub fn deinit(_: *Self) void {}

fn insert(self: *Self, _: K, _: V, level: usize) !void {
    assert(level > 0 and level <= MAX_LEVEL);
    var i: usize = level - 1;
    while (i > 0) {
        for (self.list[i].items) |ptr| {
            switch (std.mem.order(u8, ptr.key, )) {}
        }
    }
}

pub fn get(_: Self, _: K) ?V {
    return null;
}

const Iterator = struct {};

pub fn iterator(_: *const Self) Iterator {}

const Node = struct {
    key: K,
    value: V,
    next: [MAX_LEVEL]?*Node = [_]?*Node{null} ** MAX_LEVEL,
};

fn random_level() usize {
    var level: usize = 1;
    while (prng.random().float(f32) < 0.5) {
        level += 1;
    }
    return @min(level, MAX_LEVEL);
}

const testing = std.testing;
const expect = testing.expect;
test {
    var list = Self.init(testing.allocator);

    try list.insert("a", "James", 3);
    _ = list.get("a");

    try list.insert("b", "United Kingdom", 2);
    _ = list.get("b");
}

// sentinel: ?Node = null,

// pub fn insert(self: *Self, node: *Node) void {
//     const level = random_level();
//     _ = level;
//     _ = self;
//     _ = node;
// }

// pub fn find_prev(self: *Self, key: []const u8) ?[MAX_LEVEL]?*Node {
//     if (self.sentinel == null) return null;

//     var links = [_]?*Node{null} ** MAX_LEVEL;

//     var level: usize = MAX_LEVEL - 1;
//     var curr = &self.sentinel.?;
//     while (true) {
//         const next = curr.forward[level];
//         if (next != null and std.mem.order(u8, next.?.key, key) == .lt) {
//             curr = next.?;
//             continue;
//         }

//         links[level] = curr;
//         if (level == 0) return links;
//         level -= 1;
//     }
// }

// fn random_level() usize {
//     var level: usize = 1;
//     while (prng.random().float(f32) < 0.5) {
//         level += 1;
//     }
//     return @min(level, MAX_LEVEL);
// }

// fn seed(s: u64) void {
//     prng.seed(s);
// }

// const expect = std.testing.expect;
// test {
//     seed(@intCast(std.time.microTimestamp()));
//     var pool = std.heap.MemoryPool(Node).init(std.testing.allocator);
//     defer pool.deinit();

//     var list = Self{};

//     const node = try pool.create();
//     node.* = .{ .key = "name", .value = "James" };
//     list.insert(node);

//     _ = list.find_prev("name");
// }
