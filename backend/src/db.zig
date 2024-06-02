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
    username: []const u8,
};

const UnserializedUser = struct {
    id: []const u8,
    username: []const u8,
};

pub const CreateUserRequest = struct {
    username: []const u8,
};

pub const DbConnection = union(enum) {
    redis: RedisConnection,
    pub fn connect(self: *DbConnection, host: []const u8, port: u16) !void {
        switch (self.*) {
            inline else => return self.connect(host, port),
        }
    }

    pub fn getUser(self: DbConnection, allocator: Allocator, id: []const u8) !?User {
        switch (self) {
            inline else => |case| return case.getUser(allocator, id),
        }
    }

    pub fn setUser(self: DbConnection, allocator: Allocator, user: User) !void {
        switch (self) {
            inline else => |case| return case.setUser(allocator, user),
        }
    }
};

pub const RedisConnection = struct {
    _conn: [*c]hiredis.struct_redisContext = 0,

    pub const errors = error{
        FailedToConnect,
        FailedToGet,
        FailedToSet,
    };

    pub fn init() RedisConnection {
        return RedisConnection{};
    }

    pub fn deinit(self: RedisConnection) void {
        if (self._conn != 0) {
            hiredis.redisFree(self._conn);
        }
    }

    pub fn connect(self: *RedisConnection, host: [:0]const u8, port: u16) !void {
        if (self._conn == 0) {
            self._conn = hiredis.redisConnect(host, port);
        } else {
            _ = hiredis.redisReconnect(self._conn);
        }
        if (self._conn == null or self._conn.*.err != 0) {
            self.logRedisError();
            return errors.FailedToConnect;
        }
    }

    fn reconnect(self: RedisConnection) void {
        _ = hiredis.redisReconnect(self._conn);
    }

    fn logRedisError(self: RedisConnection) void {
        assert(self._conn != 0);
        logz.err()
            .string("database", "redis")
            .string("errmsg", &self._conn.*.errstr).log();
        if (self._conn.*.err != 0) {
            self.reconnect();
        }
    }

    pub fn getUser(self: RedisConnection, allocator: Allocator, id: []const u8) !?User {
        // ensure string is zero padded
        const user_id = try allocator.dupeZ(u8, id);
        defer allocator.free(user_id);

        const _reply = hiredis.redisCommand(self._conn, "GET user:%s", user_id.ptr) orelse {
            self.logRedisError();
            return errors.FailedToGet;
        };
        const redis_reply: *hiredis.struct_redisReply = @ptrCast(@alignCast(_reply));
        defer hiredis.freeReplyObject(redis_reply);

        if (redis_reply.type != hiredis.REDIS_REPLY_STRING) return null;

        const parsed_user = try std.json.parseFromSlice(UnserializedUser, allocator, redis_reply.str[0..redis_reply.len], .{});
        defer parsed_user.deinit();

        return User{
            .id = try uuid.parse(parsed_user.value.id),
            .username = try allocator.dupe(u8, parsed_user.value.username),
        };
    }

    pub fn setUser(self: RedisConnection, allocator: Allocator, user: User) !void {
        var json_user = std.ArrayList(u8).init(allocator);
        defer json_user.deinit();
        try std.json.stringify(user, .{}, json_user.writer());
        try json_user.append(0);

        var id: [37]u8 = undefined;
        @memcpy(id[0..36], &user.id.toHex(.lower));
        id[36] = 0;

        // important for the strings to be null terminated
        _ = hiredis.redisCommand(self._conn, "SET user:%s %s", &id, json_user.items.ptr) orelse {
            self.logRedisError();
            return errors.FailedToSet;
        };
    }
};
