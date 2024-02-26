const std = @import("std");

const MaxHeight = 4;

pub const SkipList = struct {
    const Node = struct {
        key: []const u8,
        value: []const u8,
        height: usize = MaxHeight,
        link: [MaxHeight]?*Node = [_]?*Node{} ** MaxHeight,
    };

    allocator: std.mem.Allocator,
    rgen: std.rand.Random,
    head: *Node,

    pub fn init(allocator: std.mem.Allocator) !SkipList {
        const head = try allocator.create(Node);

        return .{
            .allocator = allocator,
            .rgen = std.rand.DefaultPrng.init(0),
            .head = head,
        };
    }

    pub fn insert(self: *SkipList, key: []const u8, value: []const u8) void {
        var prev: [MaxHeight]*Node = undefined;
        var curr = self.head;

        var i: usize = 0;
        while (i < MaxHeight) : (i += 1) {
            prev[i] = curr;
        }

        var depth = curr.height - 1;

        while (depth >= 0) {
            prev[depth] = curr;
            var next = curr.link[depth];

            if (next == null) {
                depth -= 1;
                continue;
            }


        }
    }
};
