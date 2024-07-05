# 카테고리 선택 양식
def title_format(root_trigger_id) : 
	title_modal = {
		"type": "modal",
		"callback_id" : "modal_request_form",
		"title": {
			"type": "plain_text",
			"text": "열한시_통합_요청사항접수",
			"emoji": True
		},
		"submit": {
			"type": "plain_text",
			"text": "작성",
			"emoji": True
		},
		"close": {
			"type": "plain_text",
			"text": "취소",
			"emoji": True
		},
		"blocks": [
			{
				"type": "input",
				"block_id": "select_request_category",
				"element": {
					"type": "static_select",
					"placeholder": {
						"type": "plain_text",
						"text": "카테고리를 선택하세요.",
						"emoji": True
					},
					"options": [
						{
							"text": {
								"type": "plain_text",
								"text": "객실 추가 / 객실 정보 수정 / 단가 수정",
								"emoji": True
							},
							"value": "value-0"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "고객사(핸디즈 외) 협의 사항",
								"emoji": True
							},
							"value": "value-1"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "린넨 업무",
								"emoji": True
							},
							"value": "value-2"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "세탁 업체 협의 사항",
								"emoji": True
							},
							"value": "value-3"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "업무자동화 오류 수정",
								"emoji": True
							},
							"value": "value-4"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "증명서 요청",
								"emoji": True
							},
							"value": "value-5"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "지점 카카오 채널 가이드",
								"emoji": True
							},
							"value": "value-6"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "청소용품 업무",
								"emoji": True
							},
							"value": "value-7"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "카트 업무",
								"emoji": True
							},
							"value": "value-8"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "키퍼 모집",
								"emoji": True
							},
							"value": "value-9"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "포인트 정책(추가/차감)",
								"emoji": True
							},
							"value": "value-10"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "핸디즈 협의 사항",
								"emoji": True
							},
							"value": "value-12"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "현장 문제 해결",
								"emoji": True
							},
							"value": "value-13"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "(플랫폼) 관제페이지-kcms 오류 제보",
								"emoji": True
							},
							"value": "value-14"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "(플랫폼) 키퍼앱 오류 제보",
								"emoji": True
							},
							"value": "value-15"
						},
					],
					"action_id": "select-action"
				},
				"label": {
					"type": "plain_text",
					"text": "대표 카테고리",
					"emoji": True
				}
			}
		],
		"private_metadata": f"{root_trigger_id}"
	}

	return title_modal

# 요청 내용 결과 작성 양식
def result_format(channel_id, thread_ts, root_trigger_id, today) : 
	result_modal = {
		"type": "modal",
		"callback_id" : "result_data",
		"title": {
			"type": "plain_text",
			"text": "결과 작성 양식",
			"emoji": True
		},
		"submit": {
			"type": "plain_text",
			"text": "제출",
			"emoji": True
		},
		"close": {
			"type": "plain_text",
			"text": "취소",
			"emoji": True
		},
		"blocks": [
			{
				"type": "input",
				"block_id": "date_block",
				"element": {
					"type": "datepicker",
					"initial_date": f"{today}",
					"placeholder": {
						"type": "plain_text",
						"text": "Select a date",
						"emoji": True
					},
					"action_id": "datepicker-action"
				},
				"label": {
					"type": "plain_text",
					"text": "답변일",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "content_block",
				"element": {
					"type": "plain_text_input",
					"multiline": True,
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "결과 작성",
					"emoji": True
				}
			}
		],
		"private_metadata": f"{channel_id},{thread_ts},{root_trigger_id}"
	}

	return result_modal

# 일반 요청 양식 
def basic_format(root_trigger_id, title, request_content_name, today) : 
	basic_modal = {
		"type": "modal",
		"callback_id" : "basic_data",
		"title": {
			"type": "plain_text",
			"text": f"{title}",
			"emoji": True
		},
		"submit": {
			"type": "plain_text",
			"text": "제출",
			"emoji": True
		},
		"close": {
			"type": "plain_text",
			"text": "취소",
			"emoji": True
		},
		"blocks": [
			{
				"type": "input",
				"block_id": "date_block",
				"element": {
					"type": "datepicker",
					"initial_date": f"{today}",
					"placeholder": {
						"type": "plain_text",
						"text": "Select a date",
						"emoji": True
					},
					"action_id": "datepicker-action"
				},
				"label": {
					"type": "plain_text",
					"text": "요청일",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "branch_block",
				"element": {
					"type": "plain_text_input",
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "요청 지점",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "content_block",
				"element": {
					"type": "plain_text_input",
					"multiline": True,
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": f"{request_content_name}",
					"emoji": True
				}
			}
		],
		"private_metadata": f"{root_trigger_id}"
	}

	return basic_modal

# 첨부 링크/파일 요청 양식
def attachment_format(root_trigger_id, title, today) : 
	attachment_modal = {
		"type": "modal",
		"callback_id" : "attachment_data",
		"title": {
			"type": "plain_text",
			"text": f"{title}",
			"emoji": True
		},
		"submit": {
			"type": "plain_text",
			"text": "제출",
			"emoji": True
		},
		"close": {
			"type": "plain_text",
			"text": "취소",
			"emoji": True
		},
		"blocks": [
			{
				"type": "input",
				"block_id": "date_block",
				"element": {
					"type": "datepicker",
					"initial_date": f"{today}",
					"placeholder": {
						"type": "plain_text",
						"text": "Select a date",
						"emoji": True
					},
					"action_id": "datepicker-action"
				},
				"label": {
					"type": "plain_text",
					"text": "요청일",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "branch_block",
				"element": {
					"type": "plain_text_input",
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "요청 지점",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "content_block",
				"element": {
					"type": "plain_text_input",
					"multiline": True,
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "요청 사항",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "link_block",
				"element": {
					"type": "plain_text_input",
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "관련 문서 링크 - 사진은 스레드에 첨부해 주세요!!!!",
					"emoji": True
				}
			}
		],
		"private_metadata": f"{root_trigger_id}"
	}

	return attachment_modal

# 증명서 요청 양식
def certification_format(root_trigger_id, title, today) :
	certification_modal = {
		"type": "modal",
		"callback_id" : "certification_data",
		"title": {
			"type": "plain_text",
			"text": f"{title}",
			"emoji": True
		},
		"submit": {
			"type": "plain_text",
			"text": "제출",
			"emoji": True
		},
		"close": {
			"type": "plain_text",
			"text": "취소",
			"emoji": True
		},
		"blocks": [
			{
				"type": "input",
				"block_id": "date_block",
				"element": {
					"type": "datepicker",
					"initial_date": f"{today}",
					"placeholder": {
						"type": "plain_text",
						"text": "Select a date",
						"emoji": True
					},
					"action_id": "datepicker-action"
				},
				"label": {
					"type": "plain_text",
					"text": "요청일",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "branch_block",
				"element": {
					"type": "plain_text_input",
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "키퍼 소속 지점",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "type_block",
				"element": {
					"type": "static_select",
					"placeholder": {
						"type": "plain_text",
						"text": "선택해 주세요.",
						"emoji": True
					},
					"options": [
						{
							"text": {
								"type": "plain_text",
								"text": "위촉증명서",
								"emoji": True
							},
							"value": "value-0"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "해촉증명서",
								"emoji": True
							},
							"value": "value-1"
						}
					],
					"action_id": "static_select-action"
				},
				"label": {
					"type": "plain_text",
					"text": "증명서 종류",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "name_block",
				"element": {
					"type": "plain_text_input",
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "키퍼 이름",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "end_block",
				"element": {
					"type": "datepicker",
					"initial_date": f"{today}",
					"placeholder": {
						"type": "plain_text",
						"text": "Select a date",
						"emoji": True
					},
					"action_id": "datepicker-action"
				},
				"label": {
					"type": "plain_text",
					"text": "업무 종료년월일",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "use_block",
				"element": {
					"type": "plain_text_input",
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "증명서 용도",
					"emoji": True
				}
			}
		],
		"private_metadata": f"{root_trigger_id}"
	}

	return certification_modal

# 모집 요청 양식
def recruit_format(root_trigger_id, title, today) : 
	recruit_modal = {
		"type": "modal",
		"callback_id" : "recruit_data",
		"title": {
			"type": "plain_text",
			"text": f"{title}",
			"emoji": True
		},
		"submit": {
			"type": "plain_text",
			"text": "제출",
			"emoji": True
		},
		"close": {
			"type": "plain_text",
			"text": "취소",
			"emoji": True
		},
		"blocks": [
			{
				"type": "input",
				"block_id": "date_block",
				"element": {
					"type": "datepicker",
					"initial_date": f"{today}",
					"placeholder": {
						"type": "plain_text",
						"text": "Select a date",
						"emoji": True
					},
					"action_id": "datepicker-action"
				},
				"label": {
					"type": "plain_text",
					"text": "요청일",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "branch_block",
				"element": {
					"type": "plain_text_input",
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "모집 지점",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "type_block",
				"element": {
					"type": "static_select",
					"placeholder": {
						"type": "plain_text",
						"text": "Select an item",
						"emoji": True
					},
					"options": [
						{
							"text": {
								"type": "plain_text",
								"text": "키퍼",
								"emoji": True
							},
							"value": "value-0"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "러너",
								"emoji": True
							},
							"value": "value-1"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "하우스맨",
								"emoji": True
							},
							"value": "value-2"
						}
					],
					"action_id": "static_select-action"
				},
				"label": {
					"type": "plain_text",
					"text": "모집 포지션",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "week_block",
				"element": {
					"type": "checkboxes",
					"options": [
						{
							"text": {
								"type": "plain_text",
								"text": "평일",
								"emoji": True
							},
							"value": "value-0"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "주말",
								"emoji": True
							},
							"value": "value-1"
						},
						{
							"text": {
								"type": "plain_text",
								"text": "공휴일",
								"emoji": True
							},
							"value": "value-2"
						}
					],
					"action_id": "checkboxes-action"
				},
				"label": {
					"type": "plain_text",
					"text": "필요 요일",
					"emoji": True
				}
			},
			{
				"type": "input",
				"block_id": "ads_block",
				"element": {
					"type": "plain_text_input",
					"action_id": "plain_text_input-action"
				},
				"label": {
					"type": "plain_text",
					"text": "유료공고 기간",
					"emoji": True
				}
			}
		],
		"private_metadata": f"{root_trigger_id}"
	}

	return recruit_modal

# 결과 작성 권한 경고 양식
def result_alert_format() : 
	result_alert_modal = {
		"type": "modal",
		"callback_id" : "alert_close",
		"title": {
			"type": "plain_text",
			"text": "경고",
			"emoji": True
		},
		"submit": {
			"type": "plain_text",
			"text": "확인",
			"emoji": True
		},
		"close": {
			"type": "plain_text",
			"text": "취소",
			"emoji": True
		},
		"blocks": [
			{
				"type": "section",
				"text": {
					"type": "mrkdwn",
					"text": "작성 권한이 없습니다."
				}
			}
		]
	}

	return result_alert_modal
