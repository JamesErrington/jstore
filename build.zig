const std = @import("std");
const zdt = @import("vendor/zig-datetime/src/main.zig");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "tasiadb",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });

    // Needed for the c allocator
    exe.linkLibC();

    b.installArtifact(exe);

    {
        const options = b.addOptions();
        options.addOption([]const u8, "name", "TasiaDB");
        options.addOption([]const u8, "version", "0.0.1");

        const now = zdt.datetime.Datetime.now();
        const date_str = try std.fmt.allocPrint(b.allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{ now.date.year, now.date.month, now.date.day });
        options.addOption([]const u8, "date", date_str);
        const time_str = try std.fmt.allocPrint(b.allocator, "{d:0>2}:{d:0>2}:{d:0>2}", .{ now.time.hour, now.time.minute, now.time.second });
        options.addOption([]const u8, "time", time_str);

        exe.root_module.addOptions("build", options);
    }

    {
        const run_cmd = b.addRunArtifact(exe);
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("run", "Run the application");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const fmt_cmd = b.addFmt(.{ .paths = &.{"."}, .exclude_paths = &.{"vendor/"}, .check = false });

        const fmt_step = b.step("fmt", "Format .zig files");
        fmt_step.dependOn(&fmt_cmd.step);
    }
}
