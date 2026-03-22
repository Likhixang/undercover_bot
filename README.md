# Undercover Bot

Telegram 群聊版「谁是卧底」机器人，基于 Python、aiogram 3 和 Redis 实现，支持积分押注、白板玩法、发言超时裁定、自动投票收尾，以及群内面板化操作。

## Features

- 面板式交互：创建房间、加入、开局、查看状态、解散都可直接点击按钮
- 发言轮转：按顺序推进当前发言人，自动记录本轮发言摘要
- 超时裁定：发言超时直接出局，投票超时自动投自己
- 自动推进：无人可继续发言时自动进入投票，投票结束后自动进入下一轮或结算
- 白板玩法：白板没有词，被投出后可限时私聊猜词，猜中可共享获胜奖励
- 多卧底支持：人数增加时自动分配多名卧底与白板
- 积分押注：开局自动扣除押注，胜利阵营平分奖池
- 共享积分：可接入外部 Redis，与其他机器人共用积分账户
- 维护能力：支持超管强制停局、停机维护、停机补偿

## Gameplay

1. 玩家先私聊机器人发送 `/start`，确保开局时能收到身份词。
2. 群内发送 `/uc_help` 查看帮助，或用 `/uc_new` 创建房间。
3. 其他玩家加入房间，人数满足要求后由房主手动开局，或等待自动开局。
4. 机器人私聊发放身份：
   - 平民收到平民词
   - 卧底收到卧底词
   - 白板没有词
5. 发言阶段按顺序轮流描述，不能直接说出词。
6. 全员发言后进入投票，票出玩家后根据存活身份继续下一轮或直接结算。
7. 白板若被投出，可在限定时间内私聊机器人猜词争取奖励资格。

## Commands

- `/uc_help` 查看帮助
- `/uc_new` 创建房间
- `/uc_join` 加入房间
- `/uc_leave` 退出房间；对局中会二次确认
- `/uc_start` 房主开局
- `/uc_say 你的描述` 提交发言
- `/uc_status` 查看当前房间状态
- `/uc_bal` 查看积分余额
- `/uc_end` 房主解散房间
- `/uc_force_stop` 超管强制终止对局
- `/uc_maintain` 超管开启维护模式
- `/uc_compensate [说明]` 超管发放停机补偿

## Configuration

先复制模板：

```bash
cp .env.example .env
```

关键环境变量：

- `BOT_TOKEN`: Telegram Bot Token
- `RUN_MODE`: `polling` 或 `webhook`
- `WEBHOOK_BASE_URL`: webhook 模式下的公开访问地址
- `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB`: 主 Redis 配置
- `POINTS_REDIS_HOST` 等：共享积分 Redis，可留空
- `ALLOWED_CHAT_ID` / `ALLOWED_THREAD_ID`: 可选，限制 bot 只在指定群或话题中响应
- `SUPER_ADMIN_IDS`: 超管用户 ID 列表，逗号分隔
- `MIN_PLAYERS` / `MAX_PLAYERS`: 开局人数范围
- `BET_PER_PLAYER`: 每局押注
- `SPEAK_TIMEOUT_SECONDS`: 发言超时
- `SPEAK_REMIND_BEFORE_SECONDS`: 发言提醒阈值
- `AUTO_START_IDLE_SECONDS`: 大厅自动开局等待时间
- `VOTING_TIMEOUT_SECONDS`: 投票超时
- `WORD_PAIRS`: 词库，格式 `词1|词2;词3|词4`

## Run

使用 Docker Compose：

```bash
docker compose up -d --build
```

如果代码目录已通过 volume 挂载进容器，修改 `.py` 文件后通常只需要重启 bot 容器即可生效：

```bash
docker compose restart undercover_bot
```

## Project Structure

- `main.py`: 入口、命令注册、轮询或 webhook 启动
- `handlers.py`: 命令、按钮回调、流程编排、维护逻辑
- `game.py`: 核心玩法状态机和 Redis 数据操作
- `balance.py`: 积分读写
- `config.py`: 环境变量与运行参数
- `core.py`: bot、dispatcher、Redis 初始化
- `docker-compose.yml`: 本地部署编排

## Privacy Notes

- 仓库不应提交真实的 `.env`、数据库、Redis 持久化目录或本地调试数据
- 对外分享时建议只保留 `.env.example`
- 若使用 webhook，请自行配置域名、反向代理和证书
