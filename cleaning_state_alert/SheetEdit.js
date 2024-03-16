//클리닝 준비 시트 조작 함수
function ready_cleaning(sheet_id, state_raw, state_col) {//변수: 시트아이디, 클리닝상태 셀 raw값, 클리닝상태 셀 col값
  const docs = SpreadsheetApp.openById(sheet_id)
  const ss2 = docs.getSheetByName("티켓확인")
  const today_cnt = ss2.getRange(5,3).getValue()
  today_cnt == 0 ? ss2.getRange(state_raw, state_col).setValue("클리닝 완료") : ss2.getRange(state_raw, state_col).setValue("클리닝 진행중")
};



//러너 배정하기 기능
//변수 항목: 시트아이디, 복사할 시트탭명, 붙여넣을 시트탭명, 지점명 셀, 복사 시작지점 셀
function copy_paste(sheet_id, copytab_name, pastetab_name, branch_cell, startcells) {//러너배정확정리스트 copy & paste
  const docs = SpreadsheetApp.openById(sheet_id)
  const ss = docs.getSheetByName(copytab_name)
  const ss2 = docs.getSheetByName(pastetab_name)
  const assign_cnt = ss.getRange('O6').getValue()

  const branch_name = ss.getRange(branch_cell).getValue()
  const startrow = ss.getRange(startcells)
  const paste_sheet = ss2.getRange('A1').activate()
  const pastelastrow = paste_sheet.getNextDataCell(SpreadsheetApp.Direction.DOWN).activate().offset(1,0).getRowIndex()

  const check = Browser.msgBox(branch_name + " 배정을 확정하시겠습니까?", Browser.Buttons.YES_NO)

  
//배정 조건문
  check 
  if (check == 'yes') {
    ss2.appendRow([''])
    if (assign_cnt == 1) {
      ss.getRange(startcells).activate()
      ss.getActiveRange().copyTo(ss2.getRange(pastelastrow,1),SpreadsheetApp.CopyPasteType.PASTE_VALUES, false)
      ss.getRange("I9:J").activate().clearContent()
      Browser.msgBox("배정이 확정되었습니다.")
    }
    else {
      ss.getRange(startcells).activate()
      ss.getSelection().getNextDataRange(SpreadsheetApp.Direction.DOWN).activate()
      ss.getActiveRange().copyTo(ss2.getRange(pastelastrow,1),SpreadsheetApp.CopyPasteType.PASTE_VALUES, false)
      ss.getRange("I9:J").activate().clearContent()
      Browser.msgBox("배정이 확정되었습니다.")
    } 
  }
  else {
    Browser.msgBox("배정이 취소되었습니다.")
  }  
};



function test_fn() {//러너배정확정리스트 copy & paste
  const docs = SpreadsheetApp.openById("1hORvQKiV1vpbiPGxn-IpxK0KRAZzZ0rdWRkQfOhzMcE")
  const ss = docs.getSheetByName("선검수배정")
  const ss2 = docs.getSheetByName("배정이력")

  const branch_name = ss.getRange("B5").getValue()
  const startrow = ss.getRange("N9:V9")
  const paste_sheet = ss2.getRange('A1').activate()
  const pastelastrow = paste_sheet.getNextDataCell(SpreadsheetApp.Direction.DOWN).activate().offset(1,0).getRowIndex()
  
  //const check = Browser.msgBox(branch_name + " 배정을 확정하시겠습니까?", Browser.Buttons.YES_NO)
  
  console.log(pastelastrow)
  ss2.appendRow([''])
};
