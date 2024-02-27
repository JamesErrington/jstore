const std = @import("std");

const tdb = @import("./lib.zig");

pub fn main() !void {
    var memtable = try tdb.DB.LoadFromDir("./data/");

    std.debug.print("Name: {?s}\n", .{memtable.Get("name")});
    std.debug.print("Country: {?s}\n", .{memtable.Get("country")});
    std.debug.print("Message: {?s}\n", .{memtable.Get("message")});

    // var wal = try tdb.WAL.Create("./data/");
    // defer wal.Deinit();

    // try wal.WriteEntry(.{
    //     .key = "country",
    //     .value = "United Kingdom",
    //     .timestamp = @intCast(std.time.microTimestamp()),
    // });
    // try wal.Flush();
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
