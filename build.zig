const std = @import("std");
const datetime = @import("vendor/zig-datetime/src/main.zig");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "tasiadb",
        .root_source_file = .{ .path = "tasiadb-cli/main.zig" },
        .target = target,
        .optimize = optimize,
    });

    exe.addModule("tasiadb", b.createModule(.{
    	.source_file = .{ .path = "tasiadb/lib.zig" },
    }));

    b.installArtifact(exe);

    {
	    const options = b.addOptions();
	    options.addOption([]const u8, "version", "0.0.1");

	    const now = datetime.datetime.Datetime.now();
	    const now_str = now.formatISO8601(b.allocator, false) catch "";
	    options.addOption([]const u8, "date", now_str);

		exe.addOptions("config", options);
    }

    {
        const run_cmd = b.addRunArtifact(exe);
        if (b.args) |args| run_cmd.addArgs(args);

        const run_step = b.step("run", "Run the application");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const fmt_cmd = b.addFmt(.{ .paths = &.{"."}, .check = false });

        const fmt_step = b.step("fmt", "Format .zig files");
        fmt_step.dependOn(&fmt_cmd.step);
    }
}
