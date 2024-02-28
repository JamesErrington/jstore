const std = @import("std");

const tdb = @import("./lib.zig");

pub fn main() !void {
    var db = try tdb.DB.LoadFromDir(std.heap.c_allocator, "./data/");
    defer db.Close();

    std.debug.print("Name: {?s}\n", .{db.memtable.Get("name")});
    std.debug.print("Country: {?s}\n", .{db.memtable.Get("address")});
    std.debug.print("Message: {?s}\n", .{db.memtable.Get("age")});
    std.debug.print("MemTable size: {}\n", .{db.memtable.Size()});
}
