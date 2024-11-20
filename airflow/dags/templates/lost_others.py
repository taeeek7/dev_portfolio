class MessageTemplate :
    def slack_message(order_no, branch, room_no, end_at, image_url_groups, comment_groups, keeper) :
        blocks = []
        title = {
            "type": "section",
            "block_id": "title_block",
            "text": {
                "type": "mrkdwn",
                "text": f"*_💡 {branch}_청소완료 객실 분실물&특이사항 알림_*"
            }
        }
        contents = {
            "type": "section",
            "block_id": "contents_block",
            "text": {
                "type": "mrkdwn",
                "text":  f">오더번호: {order_no}\n"
                        +f">객실번호: {room_no}\n"
                        +f">청소완료시간: {end_at}\n"
                        +f">담당키퍼: {keeper}\n"
            }
        }
        # 분실물 & 특이사항 url for-loop
        lost_others_elements = []
        for i in range(0, len(image_url_groups)) :
            items = {
                "type": "rich_text_section",
                "elements": [
                    {
                        "type": "text",
                        "text": " "
                    },
                    {
                        "type": "link",
                        "url": f"{image_url_groups[i]}",
                        "text": f"{comment_groups[i]}",
                        "style": {
                        "bold": False
                        }
                    }
                ]
            }
            lost_others_elements.append(items)
        
        lost_others = {
            "type": "rich_text",
            "block_id": "lost_others_block",
            "elements": [
                {
                    "type": "rich_text_list",
                    "style": "bullet",
                    "indent": 0,
                    "border": 0,
                    "elements": lost_others_elements
                }
            ]
        }

        blocks.append(title)
        blocks.append(contents)
        blocks.append(lost_others)

        return blocks