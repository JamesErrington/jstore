const std = @import("std");

const tdb = @import("./lib.zig");

pub fn main() !void {
    var db = try tdb.DB.LoadFromDir(std.heap.c_allocator, "./data/");
    defer db.Close();

    std.debug.print("name: {!?s}\n", .{db.Get("name")});
    std.debug.print("country: {!?s}\n", .{db.Get("address")});
    std.debug.print("age: {!?s}\n", .{db.Get("age")});
    std.debug.print("MemTable size: {}\n", .{db.memtable.Size()});

    // try db.Put("job", "unemployed");
    std.debug.print("job: {!?s}\n", .{db.Get("job")});
    // std.debug.print("MemTable size: {}\n", .{db.memtable.Size()});
}
