# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/store/bilibili/__init__.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#

# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 19:34
# @Desc    :

from typing import List

import config
from var import source_keyword_var

from ._store_impl import *
from .bilibilli_store_media import *


class BiliStoreFactory:
    STORES = {
        "csv": BiliCsvStoreImplement,
        "db": BiliDbStoreImplement,
        "json": BiliJsonStoreImplement,
        "sqlite": BiliSqliteStoreImplement,
        "mongodb": BiliMongoStoreImplement,
        "excel": BiliExcelStoreImplement,
    }

    @staticmethod
    def create_store() -> AbstractStore:
        '''
        根据存储方式，返回存储类对象，如BiliJsonStoreImplement()
        '''
        store_class = BiliStoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError("[BiliStoreFactory.create_store] Invalid save option only supported csv or db or json or sqlite or mongodb or excel ...")
        return store_class()  # 返回 store对象，如“BiliJsonStoreImplement()”


async def update_bilibili_video(video_item: Dict):
    video_item_view: Dict = video_item.get("View")
    video_user_info: Dict = video_item_view.get("owner")
    video_item_stat: Dict = video_item_view.get("stat")
    video_id = str(video_item_view.get("aid"))
    save_content_item = {
        "video_id": video_id,  # 视频唯一标识符
        "video_type": "video",  # 内容类型，此处为视频
        "title": video_item_view.get("title", "")[:500],  # 视频标题，最多保留500个字符
        "desc": video_item_view.get("desc", "")[:500],  # 视频描述，最多保留500个字符
        "create_time": video_item_view.get("pubdate"),  # 视频创建时间（发布时间）
        "user_id": str(video_user_info.get("mid")),  # UP主用户ID
        "nickname": video_user_info.get("name"),  # UP主昵称
        "avatar": video_user_info.get("face", ""),  # UP主头像URL
        "liked_count": str(video_item_stat.get("like", "")),  # 点赞数
        "disliked_count": str(video_item_stat.get("dislike", "")),  # 点踩数
        "video_play_count": str(video_item_stat.get("view", "")),  # 播放数
        "video_favorite_count": str(video_item_stat.get("favorite", "")),  # 收藏数
        "video_share_count": str(video_item_stat.get("share", "")),  # 分享数
        "video_coin_count": str(video_item_stat.get("coin", "")),  # 投币数
        "video_danmaku": str(video_item_stat.get("danmaku", "")),  # 弹幕数
        "video_comment": str(video_item_stat.get("reply", "")),  # 评论数
        "last_modify_ts": utils.get_current_timestamp(),  # 最后修改时间戳
        "video_url": f"https://www.bilibili.com/video/av{video_id}",  # 视频链接
        "video_cover_url": video_item_view.get("pic", ""),  # 视频封面图片URL
        "source_keyword": source_keyword_var.get(),  # 来源关键词
    }
    utils.logger.info(f"[store.bilibili.update_bilibili_video] bilibili video id:{video_id}, title:{save_content_item.get('title')}")
    await BiliStoreFactory.create_store().store_content(content_item=save_content_item)  #  保存内容


async def update_up_info(video_item: Dict):
    video_item_card_list: Dict = video_item.get("Card")
    video_item_card: Dict = video_item_card_list.get("card")
    saver_up_info = {
        "user_id": str(video_item_card.get("mid")),  # 用户ID
        "nickname": video_item_card.get("name"),  # 用户昵称
        "sex": video_item_card.get("sex"),  # 性别
        "sign": video_item_card.get("sign"),  # 个人签名
        "avatar": video_item_card.get("face"),  # 头像链接
        "last_modify_ts": utils.get_current_timestamp(),  # 最后修改时间戳
        "total_fans": video_item_card.get("fans"),  # 粉丝数量
        "total_liked": video_item_card_list.get("like_num"),  # 获赞数量
        "user_rank": video_item_card.get("level_info").get("current_level"),  # 用户等级
        "is_official": video_item_card.get("official_verify").get("type"),  # 是否官方认证
    }
    utils.logger.info(f"[store.bilibili.update_up_info] bilibili user_id:{video_item_card.get('mid')}")
    await BiliStoreFactory.create_store().store_creator(creator=saver_up_info)


async def batch_update_bilibili_video_comments(video_id: str, comments: List[Dict]):
    '''
    逐条添加评论到json
    '''
    if not comments:
        return
    for comment_item in comments:
        await update_bilibili_video_comment(video_id, comment_item)


async def update_bilibili_video_comment(video_id: str, comment_item: Dict):
    '''
    json添加评论信息
    '''
    comment_id = str(comment_item.get("rpid"))
    parent_comment_id = str(comment_item.get("parent", 0))
    content: Dict = comment_item.get("content")
    user_info: Dict = comment_item.get("member")
    like_count: int = comment_item.get("like", 0)
    save_comment_item = {
        "comment_id": comment_id,  # 评论唯一标识符
        "parent_comment_id": parent_comment_id,  # 父评论ID，0表示一级评论
        "create_time": comment_item.get("ctime"),  # 评论创建时间
        "video_id": str(video_id),  # 关联的视频ID
        "content": content.get("message"),  # 评论内容
        "user_id": user_info.get("mid"),  # 评论用户ID
        "nickname": user_info.get("uname"),  # 评论用户昵称
        "sex": user_info.get("sex"),  # 评论用户性别
        "sign": user_info.get("sign"),  # 评论用户签名
        "avatar": user_info.get("avatar"),  # 评论用户头像
        "sub_comment_count": str(comment_item.get("rcount", 0)),  # 子评论数量
        "like_count": like_count,  # 点赞数量
        "last_modify_ts": utils.get_current_timestamp(),  # 最后修改时间戳
    }
    utils.logger.info(f"[store.bilibili.update_bilibili_video_comment] Bilibili video comment: {comment_id}, content: {save_comment_item.get('content')}")
    await BiliStoreFactory.create_store().store_comment(comment_item=save_comment_item)


async def store_video(aid, video_content, extension_file_name):
    """
    video video storage implementation
    Args:
        aid:
        video_content:
        extension_file_name:
    """
    await BilibiliVideo().store_video({
        "aid": aid,
        "video_content": video_content,
        "extension_file_name": extension_file_name,
    })


async def batch_update_bilibili_creator_fans(creator_info: Dict, fans_list: List[Dict]):
    if not fans_list:
        return
    for fan_item in fans_list:
        fan_info: Dict = {
            "id": fan_item.get("mid"),
            "name": fan_item.get("uname"),
            "sign": fan_item.get("sign"),
            "avatar": fan_item.get("face"),
        }
        await update_bilibili_creator_contact(creator_info=creator_info, fan_info=fan_info)


async def batch_update_bilibili_creator_followings(creator_info: Dict, followings_list: List[Dict]):
    if not followings_list:
        return
    for following_item in followings_list:
        following_info: Dict = {
            "id": following_item.get("mid"),
            "name": following_item.get("uname"),
            "sign": following_item.get("sign"),
            "avatar": following_item.get("face"),
        }
        await update_bilibili_creator_contact(creator_info=following_info, fan_info=creator_info)


async def batch_update_bilibili_creator_dynamics(creator_info: Dict, dynamics_list: List[Dict]):
    if not dynamics_list:
        return
    for dynamic_item in dynamics_list:
        dynamic_id: str = dynamic_item["id_str"]
        dynamic_text: str = ""
        if dynamic_item["modules"]["module_dynamic"].get("desc"):
            dynamic_text = dynamic_item["modules"]["module_dynamic"]["desc"]["text"]
        dynamic_type: str = dynamic_item["type"].split("_")[-1]
        dynamic_pub_ts: str = dynamic_item["modules"]["module_author"]["pub_ts"]
        dynamic_stat: Dict = dynamic_item["modules"]["module_stat"]
        dynamic_comment: int = dynamic_stat["comment"]["count"]
        dynamic_forward: int = dynamic_stat["forward"]["count"]
        dynamic_like: int = dynamic_stat["like"]["count"]
        dynamic_info: Dict = {
            "dynamic_id": dynamic_id,
            "text": dynamic_text,
            "type": dynamic_type,
            "pub_ts": dynamic_pub_ts,
            "total_comments": dynamic_comment,
            "total_forwards": dynamic_forward,
            "total_liked": dynamic_like,
        }
        await update_bilibili_creator_dynamic(creator_info=creator_info, dynamic_info=dynamic_info)


async def update_bilibili_creator_contact(creator_info: Dict, fan_info: Dict):
    save_contact_item = {
        "up_id": creator_info["id"],
        "fan_id": fan_info["id"],
        "up_name": creator_info["name"],
        "fan_name": fan_info["name"],
        "up_sign": creator_info["sign"],
        "fan_sign": fan_info["sign"],
        "up_avatar": creator_info["avatar"],
        "fan_avatar": fan_info["avatar"],
        "last_modify_ts": utils.get_current_timestamp(),
    }

    await BiliStoreFactory.create_store().store_contact(contact_item=save_contact_item)


async def update_bilibili_creator_dynamic(creator_info: Dict, dynamic_info: Dict):
    save_dynamic_item = {
        "dynamic_id": dynamic_info["dynamic_id"],
        "user_id": creator_info["id"],
        "user_name": creator_info["name"],
        "text": dynamic_info["text"],
        "type": dynamic_info["type"],
        "pub_ts": dynamic_info["pub_ts"],
        "total_comments": dynamic_info["total_comments"],
        "total_forwards": dynamic_info["total_forwards"],
        "total_liked": dynamic_info["total_liked"],
        "last_modify_ts": utils.get_current_timestamp(),
    }

    await BiliStoreFactory.create_store().store_dynamic(dynamic_item=save_dynamic_item)
