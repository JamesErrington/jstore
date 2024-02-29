const std = @import("std");

const j = @import("./lib.zig");

pub fn main() !void {
    var db = try j.DB.LoadFromDir(std.heap.c_allocator, "./data/");
    defer db.Close();

    std.debug.print("MemTable size: {}\n", .{db.memtable.Size()});

    std.debug.print("name: {!?s}\n", .{db.Get("name")});
    std.debug.print("country: {!?s}\n", .{db.Get("country")});

    std.debug.print("brother: {!?s}\n", .{db.Get("brother")});
    try db.Put("brother", "Peter Errington");
    std.debug.print("brother: {!?s}\n", .{db.Get("brother")});
}
