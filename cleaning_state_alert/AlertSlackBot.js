//클리닝진행현황 알림봇
function cleaning_alarm_handys(slack_channel, sheet_id) {//변수: 슬랙채널id , 시트아이디
  const slackToken = 'slack_token';  // 발급받은 Slack OAuth Token
  const channel = slack_channel;  // 메시지를 발송할 채널 ID
  const docs = SpreadsheetApp.openById(sheet_id)
  const ss = docs.getSheetByName("클리닝진행현황")
  const ss2 = docs.getSheetByName("티켓확인")

  //기본정보
  const branch = ss.getRange(2,2).getValue()
  const time = ss.getRange(4,3).getValue()
  const sum = ss.getRange(5,3).getValue()
  const check_in = ss.getRange(6,2).getValue()
  const check_out = ss.getRange(15,2).getValue()
  const cleaning_state = ss2.getRange(4,5).getValue()

  //체크인객실 정보
  const in_12 = ss.getRange(7,7).getValue() == 'none' ? '' : ss.getRange(7,8).getValue() + '\n\n';
  const in_13 = ss.getRange(8,7).getValue() == 'none' ? '' : ss.getRange(8,8).getValue() + '\n\n';
  const in_14 = ss.getRange(9,7).getValue() == 'none' ? '' : ss.getRange(9,8).getValue() + '\n\n';
  const in_15 = ss.getRange(10,7).getValue() == 'none' ? '' : ss.getRange(10,8).getValue() + '\n\n';
  const in_16 = ss.getRange(11,7).getValue() == 'none' ? '' : ss.getRange(11,8).getValue() + '\n\n';
  const in_17 = ss.getRange(12,7).getValue() == 'none' ? '' : ss.getRange(12,8).getValue() + '\n\n';
  const in_18 = ss.getRange(13,7).getValue() == 'none' ? '' : ss.getRange(13,8).getValue() + '\n\n';
 
  //체크아웃객실 정보
  const out = ss.getRange(16,7).getValue() == 'none' ? '' : ss.getRange(16,8).getValue() + '\n\n';

  //상품화완료
  const done = ss.getRange(18,3).getValue()

  // 발송할 메시지 내용
  const message = '🧹' + branch + '🧹 \n\n'
  + '● 데이터 기준 일시 : ' + time + '\n\n' 
  + '● 총 잔여객실 : ' + sum  + '\n\n' 
  + check_in + '\n\n' 
  + in_12 
  + in_13 
  + in_14 
  + in_15 
  + in_16 
  + in_17 
  + in_18
  + '\n'
  + check_out + '\n\n'
  + out
  + '\n'
  + '3. 상품화완료(청소+검수) : ' + done + '\n'; 

  //슬랙메시지발송필수항목
  const url = 'https://slack.com/api/chat.postMessage';
  const payload = {
    token: slackToken,
    channel: channel,
    text: message
  };
  
  const options = {
    method: 'post',
    payload: payload
  };

  //발송조건시간설정
  const now_hour = Utilities.formatDate(new Date(),"GMT+9","HH")
  const now_min = Utilities.formatDate(new Date(),"GMT+9","mm")
  now_hour >= 11 && now_hour < 18 && cleaning_state == '클리닝 진행중' 
  ? UrlFetchApp.fetch(url, options) 
  : console.log("발송시간이 아닙니다.")
};



//분실물&특이사항 알림봇
function lost_other(slack_channel, sheet_id) {//변수: 슬랙채널id , 시트아이디
  const slackToken = 'slack_token';  // 발급받은 Slack OAuth Token
  const channel = slack_channel;  // 메시지를 발송할 채널 ID
  const docs = SpreadsheetApp.openById(sheet_id)
  const ss2 = docs.getSheetByName("티켓확인")

  //변수설정
  const today = Utilities.formatDate(new Date(),"GMT+9","MM/dd") 
  const cleaning_state = ss2.getRange(4,5).getValue()
  const resv_ing = ss2.getRange(5,5).getValue()
  const inspect = ss2.getRange(6,5).getValue()
  const inspect_room = ss2.getRange(6,6).getValue()
  const lost = ss2.getRange(7,5).getValue()
  const other = ss2.getRange(8,5).getValue()
  const error_check = ss2.getRange(10,2).getValue()

  // 발송할 메시지 내용
  const message = '✨ ' + today + ' 클리닝 종료' + ' ✨\n\n' 
  + '● 검수필요: ' +  inspect   +  '건  ' + inspect_room + '\n\n'    
  + '● 분실물: ' +  lost   +  '건\n\n'
  + '● 특이사항: ' +  other   +  '건\n\n'
  + '※ 검수필요 객실을 제외한 분실물 & 특이사항 건수입니다.'  

  //console.log(message)

  //슬랙메시지발송환경설정
  const slack_url = 'https://slack.com/api/chat.postMessage';
  const payload = {
    token: slackToken,
    channel: channel,
    text: message
  };
  
  const options = {
    method: 'post',
    payload: payload
  };

  //발송조건설정
  const now_hour = Utilities.formatDate(new Date(),"GMT+9","HH")
  const now_min = Utilities.formatDate(new Date(),"GMT+9","mm")
  error_check != '없음' && now_hour >= 12 && now_min >= 30 && resv_ing == 0 && cleaning_state == '클리닝 진행중'
  ? UrlFetchApp.fetch(slack_url, options) && ss2.getRange(4,5).setValue('클리닝 완료')
  : console.log("클리닝 진행중입니다.")
};

