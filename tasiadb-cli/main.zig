const std = @import("std");

const repl = @import("./repl.zig");

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();

    try repl.Repl.start(&arena);
}
