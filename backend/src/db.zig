const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const hiredis = @cImport({
    @cInclude("hiredis.h");
});
const logz = @import("logz");
const uuid = @import("zul").UUID;

pub const User = struct {
    id: uuid,
    name: []const u8,
};

pub const DbConnection = union(enum) {
    redis: RedisConnection,
    pub fn connect(self: *DbConnection, host: []const u8, port: u16) !void {
        switch (self.*) {
            inline else => return self.connect(host, port),
        }
    }

    pub fn getUser(self: *DbConnection, id: uuid) !void {
        switch (self.*) {
            inline else => return self.getUser(id),
        }
    }
};

pub const RedisConnection = struct {
    _conn: [*c]hiredis.struct_redisContext = 0,
    _allocator: Allocator,

    pub const errors = error{
        FailedToConnect,
        FailedToGET,
    };

    pub fn init(allocator: Allocator) RedisConnection {
        return RedisConnection{ ._allocator = allocator };
    }

    fn logRedisError(self: RedisConnection) void {
        assert(self._conn != 0);
        logz.err()
            .string("database", "redis")
            .string("errmsg", &self._conn.*.errstr).log();
    }

    pub fn connect(self: *RedisConnection, host: [:0]const u8, port: u16) !void {
        self._conn = hiredis.redisConnect(host, port);
        if (self._conn == null or self._conn.*.err != 0) {
            self.logRedisError();
            return errors.FailedToConnect;
        }
    }

    /// Executes redis GET command with the given key and returns a
    /// caller owned string.
    fn redisGet(self: *RedisConnection, allocator: Allocator, key: anytype) ![]u8 {
        assert(self._conn != 0);

        var string = std.ArrayList(u8).init(allocator);
        errdefer string.deinit();
        try std.json.stringify(key, .{}, string.writer());

        std.debug.print("str = {s}\n", .{string.items});
        const _reply = hiredis.redisCommand(self._conn, "GET %s", string.items.ptr) orelse {
            self.logRedisError();
            return errors.FailedToConnect;
        };
        var redisReply: *hiredis.struct_redisReply = @ptrCast(@alignCast(_reply));
        defer hiredis.freeReplyObject(redisReply);

        // Write response string into previous array
        string.clearRetainingCapacity();
        try string.ensureTotalCapacity(redisReply.len);
        string.appendSliceAssumeCapacity(redisReply.str[0..redisReply.len]);
        return try string.toOwnedSlice();
    }

    pub fn getUser(self: *RedisConnection, id: uuid) !void {
        _ = id;
        const user = try self.redisGet(self._allocator, "user:1");
        logz.info().string("user", user).log();
    }
};
