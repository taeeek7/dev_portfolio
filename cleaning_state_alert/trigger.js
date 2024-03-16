//알림봇 트리거 설정 함수
function set_trigger(cl_cd_br) {//변수: 클리닝알림봇 함수명
  const time = new Date()
  time.setMinutes(30)
  const now_hour = Utilities.formatDate(new Date(),"GMT+9","HH")
  const now_min = Utilities.formatDate(new Date(),"GMT+9","mm")
  const alert_term = now_hour >= 11 && now_hour < 18 && now_min >= 0 && now_min <= 30
  alert_term ? ScriptApp.newTrigger(cl_cd_br).timeBased().at(time).create() : console.log("트리거 생성 시간이 아닙니다.");
};



//사용 트리거 삭제 함수
function deleteInactiveTriggers(cl_cd_br) {//변수: 클리닝알림봇 함수명
  const triggers = ScriptApp.getProjectTriggers();

  triggers.forEach(function (trigger) {
    if (trigger.getHandlerFunction() === cl_cd_br) { // 여기에 삭제할 핸들러 함수 이름을 지정
      ScriptApp.deleteTrigger(trigger);
      console.log("트리거가 삭제되었습니다.");
    }
  });
};
