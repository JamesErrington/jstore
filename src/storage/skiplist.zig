const std = @import("std");
const assert = std.debug.assert;

const Self = @This();

const MAX_HEIGHT = 4;

const Node = struct {
    key: []const u8,
    value: []const u8,
    height: usize,
    next: [MAX_HEIGHT]?*Node,
};

sentinel: Node,
randgen: std.Random.DefaultPrng,

pub fn init() Self {
    return .{
        .sentinel = .{
            .key = "",
            .value = "",
            .height = MAX_HEIGHT,
            .next = [_]?*Node{null} ** MAX_HEIGHT,
        },
        .randgen = std.Random.DefaultPrng.init(0),
    };
}

pub fn find(self: Self, key: []const u8) ?[]const u8 {
    const prev = self.find_pred_node(key);

    if (prev.next.items[0]) |node| {
        return node.value;
    }

    return null;
}

fn find_pred_node(self: Self, key: []const u8) Node {
    var node = self.sentinel;

    var r = node.next.capacity;
    while (r >= 0) {
        while (node.height <= r and std.mem.order(u8, node.next.items[r].key, key) == .lt) {
            node = node.next.items[r];
        }
        r -= 1;
    }

    return node;
}

pub fn insert(self: *Self, key: []const u8, value: []const u8) void {
    const level = self.random_level();
    assert(level < MAX_HEIGHT);
    _ = key;
    _ = value;
}

fn random_level(self: *Self) usize {
    var i: usize = 0;
    while (i < MAX_HEIGHT and self.randgen.intRangeLessThan(u8, 0, 2) % 2 == 0) {
        i += 1;
    }

    return i;
}

const expect = std.testing.expect;
test {
    const list = Self.init();
    try expect(list.sentinel.height == MAX_HEIGHT);
}
