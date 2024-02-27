const std = @import("std");

const Self = @This();
const AllocError = std.mem.Allocator.Error;

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

pub fn deinit(self: Self) void {
    self.delete_subtree(self.root);
}

pub fn insert(self: *Self, key: []const u8, value: []const u8) AllocError!bool {
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
            .eq => return false,
        }
    }

    while (curr != null and curr.?.rank == rank and std.mem.order(u8, curr.?.key, key) == .lt) {
        edge = &(curr.?.right);
        curr = curr.?.right;
    }

    const p = unzip(curr, key);
    if (curr != null and p.first == null and p.second == null) {
        return false;
    }

    const new_node = try self.alloc_node(key, value, rank, p.first, p.second);
    if (edge) |ptr| {
        ptr.* = new_node;
    } else {
        self.root = new_node;
    }

    return true;
}

pub fn search(self: Self, key: []const u8) ?[]const u8 {
    if (self.find_node(key)) |node| {
        return node.value;
    }

    return null;
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

fn alloc_node(self: Self, key: []const u8, value: []const u8, rank: u8, left: ?*Node, right: ?*Node) AllocError!*Node {
    const node = try self.allocator.create(Node);
    node.* = .{ .key = key, .value = value, .rank = rank, .left = left, .right = right };
    return node;
}

fn delete_subtree(self: Self, root: ?*Node) void {
    if (root) |node| {
        self.delete_subtree(node.left);
        self.delete_subtree(node.right);
        self.allocator.destroy(node);
    }
}

fn find_node(self: Self, key: []const u8) ?*Node {
    var curr = self.root;
    while (curr) |curr_node| {
        switch (std.mem.order(u8, key, curr_node.key)) {
            .lt => curr = curr_node.left,
            .gt => curr = curr_node.right,
            .eq => return curr_node,
        }
    }

    return null;
}

const expect = std.testing.expect;
const expectError = std.testing.expectError;
test {
    var tree = Self{ .allocator = std.testing.allocator };
    defer tree.deinit();

    try expect(tree.search("a") == null);
    try expect(try tree.insert("a", "a"));
    try expect(try tree.insert("a", "a") == false);

    var fail = Self{ .allocator = std.testing.failing_allocator };
    try expectError(AllocError.OutOfMemory, fail.insert("a", "a"));
}

test {
    var tree = Self{ .allocator = std.testing.allocator };
    defer tree.deinit();

    const keys = [_][]const u8{ "c", "b", "a" };
    for (keys) |key| {
        _ = try tree.insert(key, key);
    }

    for (keys) |key| {
        const value = tree.search(key);
        try expect(value != null);
        try expect(std.mem.eql(u8, value.?, key));
    }
}

test {
    var tree = Self{ .allocator = std.testing.allocator };
    defer tree.deinit();

    const keys = [_][]const u8{ "a", "b", "c" };
    for (keys) |key| {
        _ = try tree.insert(key, key);
    }

    for (keys) |key| {
        const value = tree.search(key);
        try expect(value != null);
        try expect(std.mem.eql(u8, value.?, key));
    }
}

test {
    var tree = Self{ .allocator = std.testing.allocator };
    defer tree.deinit();

    const keys = [_][]const u8{ "b", "a", "c" };
    for (keys) |key| {
        _ = try tree.insert(key, key);
    }

    for (keys) |key| {
        const value = tree.search(key);
        try expect(value != null);
        try expect(std.mem.eql(u8, value.?, key));
    }
}

fn check_correct(self: Self) bool {
    if (self.root) |root| {
        return check_keys(root) and check_ranks(root);
    }

    return true;
}

fn check_keys(node: *Node) bool {
    const correct_left = if (node.left) |left| check_keys_left(left, node.key) else true;
    const correct_right = if (node.right) |right| check_keys_right(right, node.key) else true;
    return correct_left and correct_right;
}

fn check_keys_left(node: *Node, key: []const u8) bool {
    if (std.mem.order(u8, node.key, key) != .lt) {
        return false;
    }

    const correct_left = if (node.left) |left| check_keys_left(left, node.key) else true;
    const correct_right = if (node.right) |right| _check_keys(right, node.key, key) else true;
    return correct_left and correct_right;
}

fn check_keys_right(node: *Node, key: []const u8) bool {
    if (std.mem.order(u8, node.key, key) != .gt) {
        return false;
    }

    const correct_left = if (node.left) |left| _check_keys(left, key, node.key) else true;
    const correct_right = if (node.right) |right| check_keys_right(right, node.key) else true;
    return correct_left and correct_right;
}

fn _check_keys(node: *Node, key_left: []const u8, key_right: []const u8) bool {
    if ((std.mem.order(u8, key_left, node.key) != .lt) or (std.mem.order(u8, key_right, node.key) != .gt)) {
        return false;
    }

    const correct_left = if (node.left) |left| _check_keys(left, key_left, node.key) else true;
    const correct_right = if (node.right) |right| _check_keys(right, node.key, key_right) else true;
    return correct_left and correct_right;
}

fn check_ranks(node: *Node) bool {
    var correct_left = true;
    var correct_right = true;

    if (node.left) |left| {
        correct_left = check_ranks(left);

        if (left.rank >= node.rank) {
            return false;
        }
    }

    if (node.right) |right| {
        correct_right = check_ranks(right);

        if (right.rank > node.rank) {
            return false;
        }
    }

    return correct_left and correct_right;
}

test {
    var tree = Self{ .allocator = std.testing.allocator };
    defer tree.deinit();

    const keys = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g" };
    for (keys) |key| {
        _ = try tree.insert(key, key);
    }

    try expect(check_correct(tree));
}

test {
    var tree = Self{ .allocator = std.testing.allocator };
    defer tree.deinit();

    const keys = [_][]const u8{ "f", "e", "g", "a", "b", "d", "c" };
    for (keys) |key| {
        _ = try tree.insert(key, key);
    }

    try expect(check_correct(tree));
}

test {
    var tree = Self{ .allocator = std.testing.allocator };
    defer tree.deinit();

    const keys = [_][]const u8{ "g", "f", "e", "d", "c", "b", "a" };
    for (keys) |key| {
        _ = try tree.insert(key, key);
    }

    try expect(check_correct(tree));
}
