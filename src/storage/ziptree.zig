const std = @import("std");

const Self = @This();

// Based on https://github.com/dominikkempa/zip-tree/tree/main

const Node = struct {
    key: []const u8,
    value: []const u8,
    rank: u8,
    left: ?*Node,
    right: ?*Node,
};

root: ?*Node = null,
randgen: std.Random.DefaultPrng = std.Random.DefaultPrng.init(0),
allocator: std.mem.Allocator,

pub fn deinit(self: *Self) void {
    self.delete_subtree(self.root);
}

pub fn insert(self: *Self, key: []const u8, value: []const u8) !void {
    const rank = self.random_rank();

    var curr = self.root;
    var edge: ?*?*Node = null;

    while (curr != null and curr.?.rank > rank) {
        switch (std.mem.order(u8, key, curr.?.key)) {
            .lt => {
                edge = &(curr.?.left);
                curr = curr.?.left;
            },
            .gt => {
                edge = &(curr.?.right);
                curr = curr.?.right;
            },
            .eq => return,
        }
    }

    while (curr != null and curr.?.rank == rank and std.mem.order(u8, key, curr.?.key) == .lt) {
        edge = &(curr.?.right);
        curr = curr.?.right;
    }

    const p = unzip(curr, key);
    if (curr != null and p.first == null and p.second == null) {
        return;
    }

    const new_node = try self.alloc_node(key, value, rank, p.first, p.second);
    if (edge) |eedge| {
        eedge.* = new_node;
    } else {
        self.root = new_node;
    }
}

fn random_rank(self: *Self) u8 {
    var rank: u8 = 0;
    while (self.randgen.random().intRangeLessThan(u8, 0, 2) % 2 == 0) {
        rank += 1;
    }

    return rank;
}

fn unzip(node: ?*Node, key: []const u8) struct { first: ?*Node, second: ?*Node } {
    if (node) |nnode| {
        switch (std.mem.order(u8, key, nnode.key)) {
            .lt => {
                const left = nnode.left;
                if (left != null and std.mem.order(u8, left.?.key, key) == .lt) {
                    const p = unzip(left.?.right, key);
                    if (left.?.right != null and p.first == null and p.second == null) {
                        return p;
                    }

                    left.?.*.right = p.first;
                    nnode.*.left = p.second;
                    return .{ .first = left, .second = nnode };
                } else {
                    const p = unzip(left, key);
                    if (left != null and p.first == null and p.second == null) {
                        return p;
                    }
                    return .{ .first = p.first, .second = nnode };
                }
            },
            .gt => {
                const right = nnode.right;
                if (right != null and std.mem.order(u8, key, right.?.key) == .lt) {
                    const p = unzip(right.?.left, key);
                    if (right.?.left != null and p.first == null and p.second == null) {
                        return p;
                    }

                    right.?.*.left = p.second;
                    nnode.*.right = p.first;
                    return .{ .first = nnode, .second = right };
                } else {
                    const p = unzip(right, key);
                    if (right != null and p.first == null and p.second == null) {
                        return p;
                    }
                    return .{ .first = nnode, .second = p.second };
                }
            },
            .eq => return .{ .first = null, .second = null },
        }
    }

    return .{ .first = null, .second = null };
}

fn alloc_node(self: Self, key: []const u8, value: []const u8, rank: u8, left: ?*Node, right: ?*Node) !*Node {
    const node = try self.allocator.create(Node);
    node.*.key = key;
    node.*.value = value;
    node.*.rank = rank;
    node.*.left = left;
    node.*.right = right;

    return node;
}

fn delete_subtree(self: *Self, root: ?*Node) void {
    if (root) |node| {
        self.delete_subtree(node.left);
        self.delete_subtree(node.right);
        self.allocator.destroy(node);
    }
}

const expect = std.testing.expect;
test {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var tree = Self{ .allocator = arena.allocator() };

    try tree.insert("b", "James");

    try expect(std.mem.eql(u8, tree.root.?.key, "b"));
    try expect(std.mem.eql(u8, tree.root.?.value, "James"));

    try tree.insert("a", "Anastasia");
    try expect(std.mem.eql(u8, tree.root.?.key, "a"));
    try expect(std.mem.eql(u8, tree.root.?.value, "Anastasia"));
}
