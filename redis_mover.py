# -*- coding:utf-8 -*- #
import redis
import logging
import argparse


def parse_protocol(url):
    source_host = str(url).split('://')[1].split('/')[0]
    source_port = str(url).split('://')[1].split('/')[0].split(':')[1] if ':' in str(url).split('://')[1].split('/')[0] else 6379
    source_db = str(url).split('://')[1].split('/')[1]
    return source_host, source_port, source_db


def start():
    logger = logging.getLogger('REDIS_MOVER')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler('redis_mover.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    terminal_handler = logging.StreamHandler()
    terminal_handler.setLevel(logging.DEBUG)
    terminal_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(terminal_handler)

    logger.info("程序启动 ⁽˙³˙⁾◟( ˘•ω•˘ )◞⁽˙³˙⁾")
    parser = argparse.ArgumentParser(description="redis_mover server example")

    parser.add_argument('--target')
    parser.add_argument('--target_pass')
    parser.add_argument('--source')
    parser.add_argument('--source_pass')
    parser.add_argument('--pattern')

    args = parser.parse_args()

    if str(args.source).split('://')[0] != 'redis':
        logger.error(f'无法识别的协议名称{str(args.source).split("://")[0]}')
        return None

    logger.info(f"准备从{args.source}服务器的数据移至{args.target}")

    target_host, target_port, target_db = parse_protocol(args.target)
    target_pass = args.target_pass

    source_host, source_port, source_db = parse_protocol(args.source)
    source_pass = args.source_pass

    remote = redis.Redis(host=target_host, password=target_pass, db=target_db, port=target_port)
    local = redis.Redis(host=source_host, password=source_pass, db=source_db, port=source_port)

    count = 0
    types = set()

    for key in local.keys(args.pattern):
        key_type = local.type(key)
        count += 1
        if key_type == b'string':
            remote.set(key, local.get(key))
            types.add(key_type)
        elif key_type == b'zset':
            for name, score in local.zrange(key, 0, -1, withscores=True):
                remote.zadd(key, name, score)
                types.add(key_type)
        elif key_type == b'hash':
            remote.hmset(key, local.hgetall(key))
            types.add(key_type)
        if local.ttl(key) and local.ttl(key) > 0:
            remote.expire(key, local.ttl(key))
    logger.info(f"共移动了{count}条数据 (>ω<)ღღღღღ")
    logger.info(f"数据类型包括{types} (̿▀̿ ̿Ĺ̯̿̿▀̿ ̿)̄")
    logger.info(f"开始检查数据完整性... (¦3ꇤ[▓▓]")
    count = 0
    bug = 0
    for key in remote.keys(args.pattern):
        count += 1
        key_type = remote.type(key)
        if key_type == b'string':
            if remote.get(key) != local.get(key) and local.ttl(key) - remote.ttl(key) > 5:
                bug += 1
                logger.error(f"{key} 数据不匹配")
                logger.error(f"local {local.get(key)} ex->{local.ttl(key)}")
                logger.error(f"remote{remote.get(key)} ex->{remote.ttl(key)}")
                logger.error("(ノಠ益ಠ)ノ 彡┻━┻")
            types.add(key_type)
        elif key_type == b'zset':
            if local.zrange(key, 0, -1, withscores=True) != remote.zrange(key, 0, -1, withscores=True) and local.ttl(
                    key) - remote.ttl(key) > 5:
                bug += 1
                logger.error(f"{key} 数据不匹配")
                logger.error(f"local {local.zrange(key,0,-1,withscores=True)}  ex->{local.ttl(key)}")
                logger.error(f"remote{remote.zrange(key,0,-1,withscores=True)}  ex->{remote.ttl(key)}")
                logger.error("(ノಠ益ಠ)ノ 彡┻━┻")
            types.add(key_type)
        elif key_type == b'hash':
            if local.hgetall(key) != remote.hgetall(key) and local.ttl(key) - remote.ttl(key) > 5:
                bug += 1
                logger.error(f"{key} 数据不匹配")
                logger.error(f"local {local.hgetall(key)} ex->{local.ttl(key)}")
                logger.error(f"remote{remote.hgetall(key)} ex->{remote.ttl(key)}")
                logger.error("(ノಠ益ಠ)ノ 彡┻━┻")
            types.add(key_type)
    logger.info(f"共检查了{count}个key ( つ•̀ω•́)つ・・*:・:・゜:==≡≡Σ=͟͟͞͞(✡)`Д´）ｸﾞﾍｯ!")
    logger.info(f"有{bug}个出现了异常 (╬•᷅д•᷄╬)")
