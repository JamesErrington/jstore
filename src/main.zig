const std = @import("std");

const tdb = @import("./lib.zig");

pub fn main() !void {
    var iter = try tdb.WALIterator.init("./test.bin");

    while (iter.next()) |entry| {
        std.debug.print("Key: '{s}'\n", .{entry.key});
    }
}

// pub fn main() !void {
//     var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
//     defer arena.deinit();

//     const allocator = arena.allocator();

//     var iter = try std.process.argsWithAllocator(allocator);
//     _ = iter.next();

//     while (iter.next()) |arg| {
//         std.debug.print("{s}\n", .{arg});
//     }
// }
