# -*- coding: utf-8 -*-
"""
简化的JSON文件解析和整合脚本
"""

import json
from typing import Dict, List, Any


def build_comment_tree(comments_data: List[Dict]) -> List[Dict]:
    """
    构建评论的父子关系树

    Args:
        comments_data: 评论数据列表

    Returns:
        构建了父子关系的评论列表（只有一级评论）
    """
    # 创建评论映射
    comment_map = {}
    root_comments = []
    all_comments = {}  # 存储所有评论，包括一级和二级

    # 第一步：先创建所有评论节点
    for comment in comments_data:
        comment_id = comment['comment_id']
        all_comments[comment_id] = {
            **comment,
            'replies': []  # 添加回复列表
        }

    # 第二步：构建父子关系
    for comment_id, comment_node in all_comments.items():
        parent_id = comment_node.get('parent_comment_id', '0')

        if parent_id == '0' or parent_id == 0:
            # 一级评论
            root_comments.append(comment_node)
            comment_map[comment_id] = comment_node
        else:
            # 二级评论，挂到父评论下
            if parent_id in comment_map:
                comment_map[parent_id]['replies'].append(comment_node)
            else:
                # 如果父评论还没创建，先创建父评论（从all_comments中获取真实数据）
                if parent_id in all_comments:
                    parent_comment = all_comments[parent_id]
                    comment_map[parent_id] = parent_comment
                else:
                    # 如果父评论不存在（数据异常），创建一个空的
                    parent_comment = {
                        'comment_id': parent_id,
                        'parent_comment_id': '0',
                        'video_id': comment_node.get('video_id', ''),
                        'replies': [],
                        'content': '',
                        'nickname': '',
                        'create_time': 0,
                        'user_id': '',
                        'sex': '',
                        'sign': '',
                        'avatar': '',
                        'like_count': 0,
                        'last_modify_ts': 0
                    }
                    comment_map[parent_id] = parent_comment

                comment_map[parent_id]['replies'].append(comment_node)

    return root_comments


def integrate_videos_comments(
    video_file: str,
    comments_file: str
) -> List[Dict]:
    """
    整合视频和评论数据

    Args:
        video_file: 视频JSON文件路径
        comments_file: 评论JSON文件路径

    Returns:
        整合后的视频评论数据列表
    """
    # 加载数据
    with open(video_file, 'r', encoding='utf-8') as f:
        video_data = json.load(f)

    with open(comments_file, 'r', encoding='utf-8') as f:
        comments_data = json.load(f)

    # 构建评论树
    print("🔨 构建评论父子关系...")
    root_comments = build_comment_tree(comments_data)

    # 创建评论映射：video_id -> 评论列表
    video_comments_map = {}
    for comment in root_comments:
        video_id = comment['video_id']
        if video_id not in video_comments_map:
            video_comments_map[video_id] = []
        video_comments_map[video_id].append(comment)

    # 整合视频和评论
    print("🔗 匹配视频和评论...")
    result = []

    for video in video_data:
        video_id = video['video_id']

        # 获取该视频的所有评论
        video_comments = video_comments_map.get(video_id, [])

        # 构建最终结构
        video_result = {
            'video_id': video_id,
            'video_info': {
                # 'video_type': video.get('video_type', ''),
                'title': video.get('title', ''),
                'desc': video.get('desc', ''),
                'create_time': video.get('create_time', 0),
                'user_id': video.get('user_id', ''),
                'nickname': video.get('nickname', ''),
                'avatar': video.get('avatar', ''),
                'liked_count': video.get('liked_count', '0'),
                'disliked_count': video.get('disliked_count', '0'),
                'video_play_count': video.get('video_play_count', '0'),
                'video_favorite_count': video.get('video_favorite_count', '0'),
                'video_share_count': video.get('video_share_count', '0'),
                'video_coin_count': video.get('video_coin_count', '0'),
                'video_danmaku': video.get('video_danmaku', '0'),
                'video_comment': video.get('video_comment', '0'),
                'last_modify_ts': video.get('last_modify_ts', 0),
                'video_url': video.get('video_url', ''),
                'video_cover_url': video.get('video_cover_url', ''),
                'source_keyword': video.get('source_keyword', '')
            },
            'comments': video_comments
        }

        result.append(video_result)

    return result


if __name__ == "__main__":
    import os
    # 地址
    path =r'J:\project\MediaCrawler\data\bili\json'
    video_file = os.path.join(path,"creator_contents_2025-12-14.json")  # 视频文件路径
    comments_file = os.path.join(path,"creator_comments_2025-12-14.json")  # 评论文件路径
    output_file = os.path.join(path,"integrated_result.json")  # 输出文件路径
    # 执行整合
    result = integrate_videos_comments(video_file, comments_file)
    # 保存结果
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"✅ 整合完成！")
    print(f"📄 输出文件: {output_file}")
    print(f"📊 视频数量: {len(result)}")
    print(f"💬 总评论数: {sum(len(v['comments']) for v in result)}")
