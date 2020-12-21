package com.cgy.realtime.bean

case class OrderInfo(
                      id: String,
                      province_id: String,
                      var consignee: String,
                      order_comment: String,
                      var consignee_tel: String,
                      order_status: String,
                      payment_way: String,
                      user_id: String,
                      img_url: String,
                      total_amount: Double,
                      expire_time: String,
                      delivery_address: String,
                      create_time: String,
                      operate_time: String,
                      tracking_no: String,
                      parent_order_id: String,
                      out_trade_no: String,
                      trade_body: String,
                      var create_date: String = null,
                      var create_hour: String = null
                    ){
  create_date=create_time.substring(0,10)
  create_hour=create_time.substring(11,13)
  // 名字脱敏
  // 李振超  =>李**    18603071634=>186****1634
  consignee = consignee.substring(0,1)+"**"
  consignee_tel=consignee_tel.replaceAll("(\\d{3})\\{4}(\\d{4})","$1****$2")
}
