------------脚本信息模版-------------
----FileName：
----DESC:testdesc
----By:
-------------脚本信息模版-------------

set hive.default.fileformat=Orc;
set mapreduce.map.output.compress=true;
set mapreduce.output.fileoutputformat.compress=true;
set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
set hive.exec.compress.output=true;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

set mapreduce.reduce.memory.mb=1025;
set mapreduce.map.memory.mb=1025;

--添加UDF的jar包
ADD JAR hdfs://master:8020/user/codemao/udf/ua-hive.jar;
--注册UDF函数
drop function ParseUserAgent;
create function ParseUserAgent
    AS 'nl.basjes.parse.useragent.hive.ParseUserAgent'
USING JAR 'hdfs://master:8020/user/codemao/udf/ua-hive.jar';


-- create external table `app_r_com_device_sum_day`
-- (
--     `data_date` string comment '统计日期'
--     ,`pro_code` string comment '产品编码'
--     ,`os_type` string comment '操作系统'
--     ,`agent` string comment '一级渠道'
--     ,`resolution`  string comment '分辨率'
--     ,`pv` bigint comment 'pv'
--     ,`uv` bigint comment 'uv'
-- )comment '通用设备相关的pv/uv用户数据'
-- partitioned by (`dt` string)
-- stored as orc
-- location '/user/hive/warehouse/app.db/app_r_com_device_sum_day';




insert overwrite table tmp.libra_codecamp_edu_cpa_day  partition (dt)


-- 录播课CPA
select data_date,package_business_type,content_type,first_channel_id,first_channel,second_channel_id,second_channel,nvl(third_channel_id,0) as third_channel_id,third_channel,city_level,
       nvl(sum(clue_num),0) as clue_num,
       nvl(sum(new_clue_num),0) as new_clue_num,
       nvl(sum(useless_clue_num),0) as useless_clue_num,
       nvl(sum(term_valid_num),0) as term_valid_num,
       '%task_date' as dt
from
    (
        select created_at as data_date,package_business_type,content_type,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,
               nvl(city_level,'其他城市') as city_level,
               count(distinct t1.user_id) as clue_num,
               count(distinct case when first_order=1 then t1.user_id else null end) as new_clue_num,
               count(distinct case when first_order!=1 or is_deleted=1 then t1.user_id else null end) as useless_clue_num,
               null as term_valid_num

        from
            (select user_id,package_business_type,content_type,source_name,category_name,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,created_at,is_deleted,term_id,first_course_id,second_course_id,third_course_id,four_course_id,first_order
             from
                 (select user_id,package_business_type,content_type,source_name,category_id,category_name,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,created_at,is_deleted,
                         term_id,first_course_id,second_course_id,third_course_id,four_course_id,
                         -- row_number() over(partition by user_id order by created_at desc) as last_order,
                         row_number() over(partition by user_id order by created_at asc) as first_order
                  from fdm.fdm_t_codecamp_purchased_package_day
                  where dt = '%task_date'
                    and attribute=1
                    and order_status not in (0,2) ) as t
             where to_date(created_at) >= '2021-01-01' ) as t1   --体验课购课时间排序 -- between add_months(trunc('%task_date','MM'),-1) and '%task_date'  and category_id in ('3','10','25')
                left join
            -- 用户城市
                (select user_id,city_id
                 from bdm.bdm_b_mysql_codecamp_tbl_user_day
                 where dt=date_sub(CURRENT_DATE, 1)
                 group by user_id,city_id) as c
            on t1.user_id=c.user_id
                left join
            -- 城市线级
                (select city_id,city_level
                 from tmp.john_analyse_city_info
                 group by city_id,city_level) as d
            on c.city_id=d.city_id
        group by created_at,package_business_type,content_type,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,nvl(city_level,'其他城市'),'%task_date'

        union all

        select term_finish_time as data_date,package_business_type,content_type,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,nvl(city_level,'其他城市') as city_level,
               0 as clue_num,
               0 as new_clue_num,
               0 as useless_clue_num,
               count (distinct case when (if(study1.course_id is null,0,1) + if(study2.course_id is null,0,1) + if(study3.course_id is null,0,1) + if(study4.course_id is null,0,1))>=2 then t1.user_id else null end ) as term_valid_num

        from
            (select user_id,package_business_type,content_type,source_name,category_name,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,term_finish_time,term_id,first_course_id,second_course_id,third_course_id,four_course_id,first_order
             from
                 (select user_id,package_business_type,content_type,source_name,category_id,category_name,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,term_finish_time,term_id,first_course_id,second_course_id,third_course_id,four_course_id,
                         -- row_number() over(partition by user_id order by term_finish_time desc) as last_order,
                         row_number() over(partition by user_id order by term_finish_time asc) as first_order
                  from fdm.fdm_t_codecamp_purchased_package_day
                  where dt = '%task_date'
                    and attribute=1
                    and order_status not in (0,2) ) as t
             where term_finish_time >= '2021-01-01') as t1   --体验课结营时间排序 --  between add_months(trunc('%task_date','MM'),-1) and '%task_date' and category_id in ('3','10','25')
            -- 上课信息
                left join
            (select substring(created_at,1,10) as created_at,user_id,term_id,course_id
             from bdm.bdm_b_mysql_codecamp_tbl_study_status_day
             where dt=date_sub(CURRENT_DATE, 1)
             group by substring(created_at,1,10),user_id,term_id,course_id) as study1
            on t1.term_id=study1.term_id and t1.first_course_id=study1.course_id and t1.user_id=study1.user_id
                left join
            (select substring(created_at,1,10) as created_at,user_id,term_id,course_id
             from bdm.bdm_b_mysql_codecamp_tbl_study_status_day
             where dt=date_sub(CURRENT_DATE, 1)
             group by substring(created_at,1,10),user_id,term_id,course_id) as study2
            on t1.term_id=study2.term_id and t1.second_course_id=study2.course_id and t1.user_id=study2.user_id
                left join
            (select substring(created_at,1,10) as created_at,user_id,term_id,course_id
             from bdm.bdm_b_mysql_codecamp_tbl_study_status_day
             where dt=date_sub(CURRENT_DATE, 1)
             group by substring(created_at,1,10),user_id,term_id,course_id) as study3
            on t1.term_id=study3.term_id and t1.third_course_id=study3.course_id and t1.user_id=study3.user_id
                left join
            (select substring(created_at,1,10) as created_at,user_id,term_id,course_id
             from bdm.bdm_b_mysql_codecamp_tbl_study_status_day
             where dt=date_sub(CURRENT_DATE, 1)
             group by substring(created_at,1,10),user_id,term_id,course_id) as study4
            on t1.term_id=study4.term_id and t1.four_course_id=study4.course_id and t1.user_id=study4.user_id
                left join
            -- 用户城市
                (select user_id,city_id
                 from bdm.bdm_b_mysql_codecamp_tbl_user_day
                 where dt=date_sub(CURRENT_DATE, 1)
                 group by user_id,city_id) as c
            on t1.user_id =c.user_id
                left join
            -- 城市线级
                (select city_id,city_level
                 from tmp.john_analyse_city_info
                 group by city_id,city_level) as d
            on c.city_id=d.city_id
        where t1.first_order=1 --取首次结营课期
        group by term_finish_time,package_business_type,content_type,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,nvl(city_level,'其他城市')
    ) as x
group by data_date,package_business_type,content_type,first_channel_id,first_channel,second_channel_id,second_channel,nvl(third_channel_id,0),third_channel,city_level,'%task_date'


union all
-- 定制课CPA

select data_date,package_business_type,content_type,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,city_level,
       sum(clue_num) as clue_num,
       sum(new_clue_num) as new_clue_num,
       sum(useless_clue_num) as useless_clue_num,
       sum(term_valid_num) as term_valid_num,
       '%task_date' as dt
from
    (select to_date(created_at) as data_date,'66' as package_business_type,'66' as content_type,a.source_id as first_channel_id,source_name as first_channel,channel_id as second_channel_id,channel_name as second_channel,0 as third_channel_id,null as third_channel,
            nvl(city_level,'其他城市') as city_level,
            count(distinct user_id) as clue_num,
            count(distinct case when first_order=1 then user_id else null end) as new_clue_num,
            count(distinct case when first_order!=1 then user_id else null end) as useless_clue_num,
            0 as term_valid_num
     from
         --体验课购课时间排序
         (select user_id,source_id,source_name,channel_id,channel_name,channel_third,city_id,created_at,last_order,first_order
          from
              (select user_id,source_id,source_name,channel_id,channel_name,channel_third,city_id,created_at,
                      row_number() over(partition by user_id order by created_at desc) as last_order,
                       row_number() over(partition by user_id order by created_at asc) as first_order
               from fdm.fdm_m_edu_user_crm_new_day
               where dt <= '%task_date'
                 and is_company=0) as t
          where to_date(created_at) >= '2021-01-01') as a   -- between add_months(trunc('%task_date','MM'),-1) and '%task_date'
             join
         --渠道类别信息提取
             (select
                  id as source_id,
                  source_category_id
              from bdm.bdm_b_mysql_tbl_customer_channel_source_day
              where dt='%task_date'
                    -- and source_category_id=4  -- 放开渠道限制
              group by id,source_category_id) as b
         on a.source_id=b.source_id
             left join
         -- 城市线级
             (select city_id,city_level
              from tmp.john_analyse_city_info
              group by city_id,city_level) as c
         on a.city_id=c.city_id
     group by to_date(created_at),a.source_id,source_name,channel_id,channel_name,channel_third,nvl(city_level,'其他城市')

     union all
     -- 拨打有效判定
     select data_date,'66' as package_business_type,'66' as content_type,a.source_id as first_channel_id,source_name as first_channel,a.channel_id as second_channel_id,channel_name as second_channel,0 as third_channel_id,null as third_channel,
            CASE
                WHEN city_name IN('北京市','上海市','广州市','深圳市') THEN '一线城市'
                WHEN city_name IN('成都市','杭州市','武汉市','天津市','南京市','重庆市','西安市','长沙市','青岛市','沈阳市','苏州市','合肥市','郑州市','佛山市','东莞市') THEN '新一线城市'
                WHEN city_name IN('大连市','厦门市','宁波市','无锡市','福州市','温州市','济南市','昆明市','太原市','南昌市','南宁市','贵阳市','长春市','泉州市','常州市','珠海市','金华市','烟台市','惠州市','徐州市','嘉兴市',
                                  '南通市','哈尔滨市','石家庄市','兰州市','绍兴市','中山市','保定市','廊坊市','台州市') THEN '二线城市'
                ELSE '其他城市' END AS city_level,
            0 as clue_num,
            0 as new_clue_num,
            0 as useless_clue_num,
            sum(term_valid_num) as term_valid_num
     from
         (select dt,data_date,source_id,channel_id,city_name,
                 sum(new_customer_cnt) - sum(not_answer_cnt) - sum(empty_cnt) - sum(abandon_cnt) as term_valid_num
          from app.app_r_edu_user_active_detail_7day
          where dt >= '2021-01-01' -- between add_months(trunc('%task_date','MM'),-1) and '%task_date'
            and data_date >= '2021-01-01'  -- between add_months(trunc('%task_date','MM'),-1) and '%task_date'
            and if(dt='%task_date',data_date<=dt,data_date=date_sub(dt,7))
          group by dt,data_date,source_id,channel_id,city_name) as a
             join
         --渠道类别信息提取
             (select id as source_id
              from bdm.bdm_b_mysql_tbl_customer_channel_source_day
              where dt='%task_date'
                    -- and source_category_id=4  -- 放开渠道限制
              group by id) as b
         on a.source_id=b.source_id
             join
         (select source_id,source_name,channel_id,channel_name
          from fdm.fdm_d_source_channel_info) as c
         on a.source_id=c.source_id and a.channel_id=c.channel_id
     group by data_date,a.source_id,source_name,a.channel_id,channel_name,
              CASE
                  WHEN city_name IN('北京市','上海市','广州市','深圳市') THEN '一线城市'
                  WHEN city_name IN('成都市','杭州市','武汉市','天津市','南京市','重庆市','西安市','长沙市','青岛市','沈阳市','苏州市','合肥市','郑州市','佛山市','东莞市') THEN '新一线城市'
                  WHEN city_name IN('大连市','厦门市','宁波市','无锡市','福州市','温州市','济南市','昆明市','太原市','南昌市','南宁市','贵阳市','长春市','泉州市','常州市','珠海市','金华市','烟台市','惠州市','徐州市','嘉兴市',
                                    '南通市','哈尔滨市','石家庄市','兰州市','绍兴市','中山市','保定市','廊坊市','台州市') THEN '二线城市'
                  ELSE '其他城市' END) AS X
group by data_date,package_business_type,content_type,first_channel_id,first_channel,second_channel_id,second_channel,third_channel_id,third_channel,city_level,'%task_date'

union all

-- 表单导入量
select to_date(created_at) as data_date,'99' as package_business_type,'99' as content_type,first_channel_id,first_channel_name as first_channel,second_channel_id,second_channel_name as second_channel,nvl(third_channel_id,0) as third_channel_id,third_channel_name as third_channel,
       nvl(city_level,'其他城市') as city_level,
       count(distinct b.user_id) as clue_num,
       count(distinct b.user_id) as new_clue_num,
       0 as useless_clue_num,
       0 as term_valid_num,
       '%task_date' as dt

from
    (select first_channel_id,first_channel_name,second_channel_id,second_channel_name,third_channel_id,third_channel_name,phone_number,created_at
     from bdm.bdm_b_mysql_db37_crm_customer_import_detail_day
     where dt='%task_date'
       and to_date(created_at) >= '2021-01-01' -- between add_months(trunc('%task_date','MM'),-1) and '%task_date'
       and result=1) as a
        join
    (select phone_number,user_id,source_business_code,city_id
     from bdm.bdm_b_mysql_db37_crm_customer_day
     where dt ='%task_date'
       and deleted = 0
     group by phone_number,user_id,source_business_code,city_id) as b
    on a.phone_number = b.phone_number
        left join
    -- 城市线级
        (select city_id,city_level
         from tmp.john_analyse_city_info
         group by city_id,city_level) as d
    on b.city_id=d.city_id
group by to_date(created_at),first_channel_id,first_channel_name,second_channel_id,second_channel_name,third_channel_id,third_channel_name,nvl(city_level,'其他城市'),'%task_date'

union all
-- 表单导入注册量
select to_date(c.created_at) as data_date,package_business_type,content_type,first_channel_id,first_channel_name as first_channel,second_channel_id,second_channel_name as second_channel,nvl(third_channel_id,0) as third_channel_id,third_channel_name as third_channel,
       nvl(city_level,'其他城市') as city_level,
       count(distinct b.user_id) as clue_num,
       count(distinct case when first_order=1 then b.user_id else null end) as new_clue_num,
       count(distinct case when first_order!=1 or is_deleted=1 then b.user_id else null end) as useless_clue_num,
       0 as term_valid_num,
       '%task_date' as dt

from
    (select first_channel_id,first_channel_name,second_channel_id,second_channel_name,third_channel_id,third_channel_name,phone_number,created_at
     from bdm.bdm_b_mysql_db37_crm_customer_import_detail_day
     where dt='%task_date'
       and result=1) as a
        join
    (select phone_number,user_id,source_business_code,city_id
     from bdm.bdm_b_mysql_db37_crm_customer_day
     where dt ='%task_date'
       and deleted = 0
     group by phone_number,user_id,source_business_code,city_id) as b
    on a.phone_number = b.phone_number
        join
    --体验课注册购买
        (select user_id,package_business_type,content_type,source_name,category_name,first_channel,created_at,is_deleted,first_order
         from
             (select user_id,package_business_type,content_type,source_name,category_id,category_name,first_channel,created_at,is_deleted,
                     row_number() over(partition by user_id order by created_at asc) as first_order
              from fdm.fdm_t_codecamp_purchased_package_day
              where dt = '%task_date'
                and attribute=1
                and order_status not in (0,2) ) as t
         where  to_date(created_at) >= '2021-01-01' ) as c -- between add_months(trunc('%task_date','MM'),-1) and '%task_date' and category_id in ('3','10','25')
    on b.user_id=c.user_id
        left join
    -- 城市线级
        (select city_id,city_level
         from tmp.john_analyse_city_info
         group by city_id,city_level) as d
    on b.city_id=d.city_id
group by to_date(c.created_at),package_business_type,content_type,first_channel_id,first_channel_name,second_channel_id,second_channel_name,nvl(third_channel_id,0),third_channel_name,nvl(city_level,'其他城市'),'%task_date'


-- create table tmp.libra_codecamp_edu_cpa_daylibra_codecamp_edu_cpa_daylibra_codecamp_edu_cpa_day

-- (data_date date comment '统计日期',
-- package_business_type string comment '课包类型 66定制课 99表单导入',
-- content_type string comment '内容类型 66定制课 99表单导入',
-- first_channel_id string comment '一级渠道ID',
-- first_channel string comment '一级渠道',
-- second_channel_id string comment '二级渠道ID',
-- second_channel string comment '二级渠道',
-- third_channel_id string comment '三级渠道_ID',
-- third_channel string comment '三级渠道',
-- city_level string comment '城市线级',
-- clue_num bigint comment '注册线索量（含重复注册和退课）',
-- new_clue_num bigint comment '首次注册线索量（规则计算对应口径）',
-- useless_clue_num bigint comment '无效注册线索量（重复or退课）',
-- term_valid_num bigint comment '有效体验线索量（完成>=2节体验课）'
-- )
-- COMMENT 'Libra_录播课&定制课&表单导入CPA结算明细(最新分区更新最近两个月数据)'
-- PARTITIONED BY ( `dt` string)
-- stored as orc
-- location '/user/hive/warehouse/tmp.db/libra_codecamp_edu_cpa_day'
