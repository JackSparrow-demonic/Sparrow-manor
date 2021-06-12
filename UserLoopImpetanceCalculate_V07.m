function [TQZK,MidResult] = UserLoopImpetanceCalculate_V07(sd,ed,qy,nearby_tg,dn)
% % 用户回路阻抗计算——用户的相位以辨识结果为准
% % 采用同相所有用户的电流为依据修正每个用户的干路电流值。
% % 以河北石家庄数据为基础
% % 允许多日数据拼接，采用一元、二元两套模型计算用户的回路阻抗；
% % V06 升级变化的内容--------------------------------------
% % （1）针对河北导回的部分台区一年四季的电压、电流数据，计算用户的
% % V07 升级变化内容--------------------------
% % （1）首先进行用户的相位关系识别，而后再执行阻抗计算,即采用类似正比的手段，先辨识出用户的相位信息

%%
% % ---离线数据导入处理




% sd=20201001;
% ed=20201031;
% qy='134010902';
% nearby_tg='(1646179)';
% dn=0;

SDate=['''',num2str(sd),''''];
EDate=['''',num2str(ed),''''];
QY=['''',qy,''''];



%
Conn_NARI=database('orcl','HEBEI','Zzkj123456', 'oracle.jdbc.driver.OracleDriver', ...
    'jdbc:oracle:thin:@192.168.0.140:1521:');


setdbprefs('DataReturnFormat','table');

sq_uv=['SELECT tg.tg_id,tg.org_no,TO_CHAR(vc.data_date,','''yyyymmdd''',')',...
    ' as DATA_DATE,edp.meter_id,edp.cons_sort,edp.wiring_mode,vc.phase_flag,',...
    ' VC.U1,	VC.U2,	VC.U3,	VC.U4,	VC.U5,	VC.U6,	VC.U7,	VC.U8,	VC.U9,	VC.U10,	VC.U11,	VC.U12,',...
    ' VC.U13,	VC.U14,	VC.U15,	VC.U16,	VC.U17,	VC.U18,	VC.U19,	VC.U20,	VC.U21,	VC.U22,	VC.U23,	VC.U24,',...
    ' VC.U25,	VC.U26,	VC.U27,	VC.U28,	VC.U29,	VC.U30,	VC.U31,	VC.U32,	VC.U33,	VC.U34,	VC.U35,	VC.U36,',...
    ' VC.U37,	VC.U38,	VC.U39,	VC.U40,	VC.U41,	VC.U42,	VC.U43,	VC.U44,	VC.U45,	VC.U46,	VC.U47,	VC.U48,',...
    ' VC.U49,	VC.U50,	VC.U51,	VC.U52,	VC.U53,	VC.U54,	VC.U55,	VC.U56,	VC.U57,	VC.U58,	VC.U59,	VC.U60,',...
    ' VC.U61,	VC.U62,	VC.U63,	VC.U64,	VC.U65,	VC.U66,	VC.U67,	VC.U68,	VC.U69,	VC.U70,	VC.U71,	VC.U72,',...
    ' VC.U73,	VC.U74,	VC.U75,	VC.U76,	VC.U77,	VC.U78,	VC.U79,	VC.U80,	VC.U81,	VC.U82,	VC.U83,	VC.U84,',...
    ' VC.U85,	VC.U86,	VC.U87,	VC.U88,	VC.U89,	VC.U90,	VC.U91,	VC.U92,	VC.U93,	VC.U94,	VC.U95,	VC.U96 ',...
    ' FROM NARI_VOL_CURVE vc ',...
    ' INNER JOIN e_data_mp edp ON vc.id=edp.id ',...
    ' INNER JOIN G_TG TG ON EDP.TG_ID=TG.TG_ID ',...
    ' WHERE TG.PUB_PRIV_FLAG = ','''01''',' AND vc.data_date>=TO_DATE(',SDate,',','''yyyymmdd''',') ',...
    ' AND vc.data_date<=TO_DATE(',EDate,',','''yyyymmdd''',') AND edp.cons_sort not like', '''06''',...
    ' AND vc.phase_flag in(1,2,3) and tg.org_no like ',QY,...
    ' AND TG.TG_ID IN ',nearby_tg,...
    ' ORDER BY tg.org_no,tg.tg_id,edp.cons_sort,vc.data_date,vc.phase_flag '];   % 用户电压

sq_tv=['SELECT tg.tg_id,tg.org_no,TO_CHAR(vc.data_date,','''yyyymmdd''',')',...
    ' as DATA_DATE,edp.meter_id,edp.cons_sort,edp.wiring_mode,vc.phase_flag,',...
    ' VC.U1,	VC.U2,	VC.U3,	VC.U4,	VC.U5,	VC.U6,	VC.U7,	VC.U8,	VC.U9,	VC.U10,	VC.U11,	VC.U12,',...
    ' VC.U13,	VC.U14,	VC.U15,	VC.U16,	VC.U17,	VC.U18,	VC.U19,	VC.U20,	VC.U21,	VC.U22,	VC.U23,	VC.U24,',...
    ' VC.U25,	VC.U26,	VC.U27,	VC.U28,	VC.U29,	VC.U30,	VC.U31,	VC.U32,	VC.U33,	VC.U34,	VC.U35,	VC.U36,',...
    ' VC.U37,	VC.U38,	VC.U39,	VC.U40,	VC.U41,	VC.U42,	VC.U43,	VC.U44,	VC.U45,	VC.U46,	VC.U47,	VC.U48,',...
    ' VC.U49,	VC.U50,	VC.U51,	VC.U52,	VC.U53,	VC.U54,	VC.U55,	VC.U56,	VC.U57,	VC.U58,	VC.U59,	VC.U60,',...
    ' VC.U61,	VC.U62,	VC.U63,	VC.U64,	VC.U65,	VC.U66,	VC.U67,	VC.U68,	VC.U69,	VC.U70,	VC.U71,	VC.U72,',...
    ' VC.U73,	VC.U74,	VC.U75,	VC.U76,	VC.U77,	VC.U78,	VC.U79,	VC.U80,	VC.U81,	VC.U82,	VC.U83,	VC.U84,',...
    ' VC.U85,	VC.U86,	VC.U87,	VC.U88,	VC.U89,	VC.U90,	VC.U91,	VC.U92,	VC.U93,	VC.U94,	VC.U95,	VC.U96 ',...
    ' FROM NARI_VOL_CURVE vc ',...
    ' INNER JOIN e_data_mp edp ON vc.id=edp.id ',...
    ' INNER JOIN G_TG TG ON EDP.TG_ID=TG.TG_ID ',...
    ' WHERE TG.PUB_PRIV_FLAG = ','''01''',' AND vc.data_date>=TO_DATE(',SDate,',','''yyyymmdd''',') ',...
    ' AND vc.data_date<=TO_DATE(',EDate,',','''yyyymmdd''',') AND edp.cons_sort= ','''06''',...
    ' AND vc.phase_flag in(1,2,3) and tg.org_no like ',QY ,...
    ' AND TG.TG_ID IN ',nearby_tg,...
    ' ORDER BY tg.org_no,tg.tg_id,edp.cons_sort,vc.data_date,vc.phase_flag '];   % 配变电压

sq_tc=['SELECT TG.TG_ID,TG.ORG_NO,TO_CHAR(CC.DATA_DATE,', '''yyyymmdd''',') AS DATA_DATE,',...
    ' EDP.METER_ID,EDP.CONS_SORT,EDP.WIRING_MODE,CC.CT,CC.MARK,CC.PHASE_FLAG, ',...
    ' CC.I1,	CC.I2,	CC.I3,	CC.I4,	CC.I5,	CC.I6,	CC.I7,	CC.I8,	CC.I9,	CC.I10,	CC.I11,	CC.I12,',...
    ' CC.I13,	CC.I14,	CC.I15,	CC.I16,	CC.I17,	CC.I18,	CC.I19,	CC.I20,	CC.I21,	CC.I22,	CC.I23,	CC.I24,',...
    ' CC.I25,	CC.I26,	CC.I27,	CC.I28,	CC.I29,	CC.I30,	CC.I31,	CC.I32,	CC.I33,	CC.I34,	CC.I35,	CC.I36,',...
    ' CC.I37,	CC.I38,	CC.I39,	CC.I40,	CC.I41,	CC.I42,	CC.I43,	CC.I44,	CC.I45,	CC.I46,	CC.I47,	CC.I48,',...
    ' CC.I49,	CC.I50,	CC.I51,	CC.I52,	CC.I53,	CC.I54,	CC.I55,	CC.I56,	CC.I57,	CC.I58,	CC.I59,	CC.I60,',...
    ' CC.I61,	CC.I62,	CC.I63,	CC.I64,	CC.I65,	CC.I66,	CC.I67,	CC.I68,	CC.I69,	CC.I70,	CC.I71,	CC.I72,',...
    ' CC.I73,	CC.I74,	CC.I75,	CC.I76,	CC.I77,	CC.I78,	CC.I79,	CC.I80,	CC.I81,	CC.I82,	CC.I83,	CC.I84,',...
    ' CC.I85,	CC.I86,	CC.I87,	CC.I88,	CC.I89,	CC.I90,	CC.I91,	CC.I92,	CC.I93,	CC.I94,	CC.I95,	CC.I96 ',...
    ' FROM NARI_CUR_CURVE cc INNER JOIN e_data_mp edp ON cc.id=edp.id ',...
    ' INNER JOIN G_TG TG ON EDP.TG_ID=TG.TG_ID WHERE TG.PUB_PRIV_FLAG = ','''01''',...
    ' AND CC.DATA_DATE >= TO_DATE(',SDate,',', '''yyyymmdd''',') ',...
    ' AND cc.data_date<=TO_DATE(',EDate,',','''yyyymmdd''',')',...
    ' AND EDP.CONS_SORT = ','''06''','AND CC.PHASE_FLAG IN (1, 2, 3) ',...
    ' AND TG.ORG_NO LIKE ',QY,...
    ' AND TG.TG_ID IN ',nearby_tg,...
    ' ORDER BY TG.ORG_NO, TG.TG_ID, EDP.CONS_SORT, CC.DATA_DATE, CC.PHASE_FLAG'];

sq_uc=['SELECT TG.TG_ID,TG.ORG_NO,TO_CHAR(CC.DATA_DATE,', '''yyyymmdd''',') AS DATA_DATE,',...
    ' EDP.METER_ID,EDP.CONS_SORT,EDP.WIRING_MODE,CC.CT,CC.MARK,CC.PHASE_FLAG, ',...
    ' CC.I1,	CC.I2,	CC.I3,	CC.I4,	CC.I5,	CC.I6,	CC.I7,	CC.I8,	CC.I9,	CC.I10,	CC.I11,	CC.I12,',...
    ' CC.I13,	CC.I14,	CC.I15,	CC.I16,	CC.I17,	CC.I18,	CC.I19,	CC.I20,	CC.I21,	CC.I22,	CC.I23,	CC.I24,',...
    ' CC.I25,	CC.I26,	CC.I27,	CC.I28,	CC.I29,	CC.I30,	CC.I31,	CC.I32,	CC.I33,	CC.I34,	CC.I35,	CC.I36,',...
    ' CC.I37,	CC.I38,	CC.I39,	CC.I40,	CC.I41,	CC.I42,	CC.I43,	CC.I44,	CC.I45,	CC.I46,	CC.I47,	CC.I48,',...
    ' CC.I49,	CC.I50,	CC.I51,	CC.I52,	CC.I53,	CC.I54,	CC.I55,	CC.I56,	CC.I57,	CC.I58,	CC.I59,	CC.I60,',...
    ' CC.I61,	CC.I62,	CC.I63,	CC.I64,	CC.I65,	CC.I66,	CC.I67,	CC.I68,	CC.I69,	CC.I70,	CC.I71,	CC.I72,',...
    ' CC.I73,	CC.I74,	CC.I75,	CC.I76,	CC.I77,	CC.I78,	CC.I79,	CC.I80,	CC.I81,	CC.I82,	CC.I83,	CC.I84,',...
    ' CC.I85,	CC.I86,	CC.I87,	CC.I88,	CC.I89,	CC.I90,	CC.I91,	CC.I92,	CC.I93,	CC.I94,	CC.I95,	CC.I96 ',...
    ' FROM NARI_CUR_CURVE cc INNER JOIN e_data_mp edp ON cc.id=edp.id ',...
    ' INNER JOIN G_TG TG ON EDP.TG_ID=TG.TG_ID WHERE TG.PUB_PRIV_FLAG = ','''01''',...
    ' AND CC.DATA_DATE >= TO_DATE(',SDate,',', '''yyyymmdd''',') ',...
    ' AND cc.data_date<=TO_DATE(',EDate,',','''yyyymmdd''',') AND TG.ORG_NO LIKE ',QY,...
    ' AND EDP.CONS_SORT != ','''06''','AND CC.PHASE_FLAG IN (1, 2, 3) ',...
    ' AND TG.TG_ID IN ',nearby_tg,...
    ' ORDER BY TG.ORG_NO, TG.TG_ID, EDP.CONS_SORT, CC.DATA_DATE, CC.PHASE_FLAG'];
sq_tp=['SELECT TG.TG_ID,TG.ORG_NO,TG.TG_CAP,TO_CHAR(PC.DATA_DATE,', '''yyyymmdd''',') AS DATA_DATE,',...
    ' EDP.METER_ID,EDP.CONS_SORT,EDP.WIRING_MODE,EDP.T_FACTOR,PC.PHASE_FLAG, ',...
    ' PC.I1,	PC.I2,	PC.I3,	PC.I4,	PC.I5,	PC.I6,	PC.I7,	PC.I8,	PC.I9,	PC.I10,	PC.I11,	PC.I12,',...
    ' PC.I13,	PC.I14,	PC.I15,	PC.I16,	PC.I17,	PC.I18,	PC.I19,	PC.I20,	PC.I21,	PC.I22,	PC.I23,	PC.I24,',...
    ' PC.I25,	PC.I26,	PC.I27,	PC.I28,	PC.I29,	PC.I30,	PC.I31,	PC.I32,	PC.I33,	PC.I34,	PC.I35,	PC.I36,',...
    ' PC.I37,	PC.I38,	PC.I39,	PC.I40,	PC.I41,	PC.I42,	PC.I43,	PC.I44,	PC.I45,	PC.I46,	PC.I47,	PC.I48,',...
    ' PC.I49,	PC.I50,	PC.I51,	PC.I52,	PC.I53,	PC.I54,	PC.I55,	PC.I56,	PC.I57,	PC.I58,	PC.I59,	PC.I60,',...
    ' PC.I61,	PC.I62,	PC.I63,	PC.I64,	PC.I65,	PC.I66,	PC.I67,	PC.I68,	PC.I69,	PC.I70,	PC.I71,	PC.I72,',...
    ' PC.I73,	PC.I74,	PC.I75,	PC.I76,	PC.I77,	PC.I78,	PC.I79,	PC.I80,	PC.I81,	PC.I82,	PC.I83,	PC.I84,',...
    ' PC.I85,	PC.I86,	PC.I87,	PC.I88,	PC.I89,	PC.I90,	PC.I91,	PC.I92,	PC.I93,	PC.I94,	PC.I95,	PC.I96 ',...
    ' FROM e_mp_power_curve pc INNER JOIN e_data_mp edp ON pc.id=edp.id ',...
    ' INNER JOIN G_TG TG ON EDP.TG_ID=TG.TG_ID WHERE TG.PUB_PRIV_FLAG = ','''01''',...
    ' AND PC.DATA_DATE >= TO_DATE(',SDate,',', '''yyyymmdd''',') ',...
    ' AND pc.data_date<=TO_DATE(',EDate,',','''yyyymmdd''',') AND TG.ORG_NO LIKE ',QY,...
    ' AND EDP.CONS_SORT = ','''06''','AND PC.PHASE_FLAG IN (1, 2, 3) ',...
    ' AND TG.TG_ID IN ',nearby_tg,...
    ' ORDER BY TG.ORG_NO, TG.TG_ID, EDP.CONS_SORT, PC.DATA_DATE, PC.PHASE_FLAG'];



UserVoltage=select(Conn_NARI,sq_uv);                          %  所有用户电压
TransVoltage=select(Conn_NARI,sq_tv);                         % 配变量测
UserCurrent=select(Conn_NARI,sq_uc);
TransCurrent=select(Conn_NARI,sq_tc);
TransCurrent((nansum(TransCurrent{:,10:105},2)==0),:)=[];
UserVoltage(strcmp(UserVoltage.WIRING_MODE,'1')&(nansum(UserVoltage{:,8:end},2)==0),:)=[];
UserCurrent(strcmp(UserCurrent.WIRING_MODE,'1')&(nansum(UserCurrent{:,10:end},2)==0),:)=[];
% TransPower=select(Conn_NARI,sq_tp);

close(Conn_NARI);

clear sq_tc sq_tp sq_tv sq_uc sq_uv


%%


% % -------------------------------台区配变量测数据处理
% ---------（1）配变电压处理
TM=stack(TransVoltage,8:103);                     % 行转列
TM.Properties.VariableNames(8:9)={'TIME','VOLT'};
TM=unstack(TM,{'VOLT'},{'PHASE_FLAG'},'NewDataVariableNames',{'UA','UB','UC'});
% TM.Properties.VariableNames(7:9)={'UA','UB','UC'};
TM=unstack(TM,{'UA','UB','UC'},'METER_ID','GroupingVariables',{'DATA_DATE','TIME'});         % 配变量测方式排布，案列展开，日期+时刻

TNM=transpose(TM.Properties.VariableNames(3:end));
TPnm=extractBefore(TNM,'_');
TTnm=extractAfter(TNM,'x');
SX=FEEL(TTnm,3);
TME=TM(:,[1:2,SX'+2]);
% --------------（2）配变电流处理
TransCurrent{:,10:end}=TransCurrent.CT .* TransCurrent{:,10:end};
TC=stack(TransCurrent,10:105);                     % 行转列
TC.Properties.VariableNames(10:11)={'CTIME','TCURRENT'};
TC=unstack(TC,{'TCURRENT'},{'PHASE_FLAG'},'NewDataVariableNames',{'IA','IB','IC'});
TC=unstack(TC,{'IA','IB','IC'},'METER_ID','GroupingVariables',{'DATA_DATE','CTIME'}); 

tcnm=transpose(TC.Properties.VariableNames(3:end));
tcph_nm=extractBefore(tcnm,'_');
tctr_nm=extractAfter(tcnm,'x');
tcsx=FEEL(tctr_nm,3);
TCME=TC(:,[1,2,tcsx'+2]);
clear TC tcnm tcph_nm tctr_nm tcsx;

% ------------- （3）量测数据档案规整
PBTG=table(str2double(TTnm(SX)),TPnm(SX),'VariableNames',{'METER_ID','PHASE'});     % 配变的台区属性；
PBTG=outerjoin(PBTG,unique(TransVoltage(:,{'TG_ID','METER_ID'})),'Keys','METER_ID',...
    'RightVariables','TG_ID','type','left');
PBTG.Properties.VariableNames(1:3)={'TransMeterID','PHASE','TGID'};                % 建档数据归集

clear TM TNM;
% --------------- 配变电压量测数据补全；
DA=fillmissing(TME{:,3:end},'movmedian',12,1);
loc=DA==0;
DA(loc)=nan;
DA=fillmissing(DA,'movmedian',12,1,'EndValues','nearest');
TME{:,3:end}=DA;
clear TransMeasure loc DA;

TME=EliminateVoltageUnbalanceEffect(TME,dn);

clear TPnm TTnm ;


%% 
UMV=stack(UserVoltage,8:103);
UMV.Properties.VariableNames(8:9)={'TIME','VOLT'};                       % 电压数据纵向展开




%% 
% --------------- 相位辨识，即拓扑识别中的的“正相比较”；

DPU=unique(UMV(:,{'ORG_NO','TG_ID','METER_ID','WIRING_MODE','PHASE_FLAG'}));
DXUser=DPU(strcmp(DPU.WIRING_MODE,'1'),{'ORG_NO','TG_ID','METER_ID','WIRING_MODE','PHASE_FLAG'});
SXUser=unique(DPU(~strcmp(DPU.WIRING_MODE,'1'),{'ORG_NO','TG_ID','METER_ID','WIRING_MODE'}));
SXUser=[SXUser,array2table(repmat(123,height(SXUser),1),'VariableNames',{'PHASE_FLAG'})];

% ---单相用户及其量测
DXU=UMV(strcmp(UMV.WIRING_MODE,'1'),:);
DXUM=unstack(DXU,'VOLT','METER_ID','GroupingVariables',{'DATA_DATE','TIME'});          % 单相用户量测表
dx_userid=str2double(extractAfter(transpose(DXUM.Properties.VariableNames(3:end)),'x')); % 从第三列开始为用户数据
DA2=fillmissing(DXUM{:,3:end},'movmedian',12,1);
loc2=DA2==0;
DA2(loc2)=nan;
DA2=fillmissing(DA2,'movmedian',12,1,'EndValues','nearest');
DXUM{:,3:end}=DA2;
clear DA2 loc2;
% ----------三相用户及其量测
SXU=UMV(~strcmp(UMV.WIRING_MODE,'1'),:);
if length(unique(SXU.PHASE_FLAG))==1
    SXUM=unstack(SXU,{'VOLT'},{'PHASE_FLAG'},'NewDataVariableNames',{'UA'});
    SXUM=[SXUM,array2table(zeros(height(SXUM),2),'VariableNames',{'UB','UC'})];
else
    SXUM=unstack(SXU,{'VOLT'},{'PHASE_FLAG'},'NewDataVariableNames',{'UA','UB','UC'});
end
SXUM=unstack(SXUM,{'UA','UB','UC'},'METER_ID','GroupingVariables',{'DATA_DATE','TIME'});
snm=transpose(SXUM.Properties.VariableNames(3:end));
sxyh=extractAfter(snm,'x');
sxsx=FEEL(sxyh,3);
SXUM=SXUM(:,[1:2,sxsx'+2]);
sx_userid=str2double(extractAfter(transpose(SXUM.Properties.VariableNames(3:end)),'x'));          % 从第4列开始为用户数据
clear UMV snm sxyh sxsx;
DA3=fillmissing(SXUM{:,3:end},'movmedian',12,1);
loc3=DA3==0;
DA3(loc3)=nan;
DA3=fillmissing(DA3,'movmedian',12,1,'EndValues','nearest');
SXUM{:,3:end}=DA3;
clear DA3 loc3;

% ---------- 单相用户相位辨识
DXXW=[];
[~,it]=unique(PBTG.TGID);
utg=PBTG.TGID(sort(it,'ascend'));
for qq=1:length(utg)
        XD=outerjoin(DXUM(:,[logical([1;1]);ismember(dx_userid,DXUser.METER_ID(DXUser.TG_ID==utg(qq)))]),...
            TME(:,[1,2,(3*qq):(3*qq+2)]),'Keys',{'DATA_DATE','TIME'},'RightVariables',...
            TME.Properties.VariableNames((3*qq):(3*qq+2)),'Type','right');
        uxg=corr(XD{:,3:end},'rows','pairwise','type','Pearson');
        dxCU=uxg(1:end-3,(end-2):end);
    [~,user_phase]=max(dxCU,[],2);
    upd=array2table([repmat(utg(qq),width(XD)-5,1),dx_userid(ismember(dx_userid,DXUser.METER_ID(DXUser.TG_ID==...
        utg(qq)))),user_phase],'VariableNames',{'TG_ID','METER_ID','JG_Phase'});
    DXXW=[DXXW;upd];
end
clear it qq XD uxg dxCU user_phase upd dx_userid;

% ------------- 三相相位辨识
SXXW=[];
for q1=1:length(utg)
    sxu=SXUser(SXUser.TG_ID==utg(q1),:);
    for q2=1:height(sxu)
        XD=outerjoin(SXUM(:,[logical([1;1]);ismember(sx_userid,sxu.METER_ID(q2))]),...
            TME(:,[1,2,(3*q1):(3*q1+2)]),'Keys',{'DATA_DATE','TIME'},'RightVariables',...
            TME.Properties.VariableNames((3*q1):(3*q1+2)),'Type','right');
        uxg=TPS(XD{:,3:5},XD{:,6:8})./3;
        [~,PhaseSequence]=max(uxg,[],2);
        SXXW=[SXXW;[utg(q1),sxu.METER_ID(q2),PhaseSequence]];
    end
end
SXXW=array2table(SXXW,'VariableNames',{'TG_ID','METER_ID','PhaseSequence'});
SQ=array2table([[1,1,1,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6]',[1,2,3,1,3,2,2,1,3,2,3,1,3,1,2,3,2,1]',...
    repmat([1;2;3],6,1)],'VariableNames',{'PhaseSequence','JG_Phase','PHASE_FLAG'});
SXXW=outerjoin(SXXW,SQ,'Keys','PhaseSequence','RightVariables',{'JG_Phase','PHASE_FLAG'},'Type','left');
SXXW=[SXXW,cell2table(cellstr(repmat('3',height(SXXW),1)),'VariableNames',{'WIRING_MODE'})];
clear DPU DXU DXUM DXUser SXU SXUM SXUser sx_userid q1 q2 XD uxg PhaseSequence sxu ;

%% 

% -------------- 用户辨识相位关联
dx_UserVoltage=UserVoltage(strcmp(UserVoltage.WIRING_MODE,'1'),:);
sx_UserVoltage=UserVoltage(strcmp(UserVoltage.WIRING_MODE,'3'),:);

dx_UserCurrent=UserCurrent(strcmp(UserCurrent.WIRING_MODE,'1'),:);
sx_UserCurrent=UserCurrent(strcmp(UserCurrent.WIRING_MODE,'3'),:);

dx_UserVoltage=outerjoin(dx_UserVoltage,DXXW,'Keys',{'TG_ID','METER_ID'},'RightVariables',...
    {'JG_Phase'},'Type','left');
sx_UserVoltage=outerjoin(sx_UserVoltage,SXXW,'Keys',{'TG_ID','METER_ID','WIRING_MODE','PHASE_FLAG'},...
    'RightVariables',{'JG_Phase'},'Type','left');

dx_UserCurrent=outerjoin(dx_UserCurrent,DXXW,'Keys',{'TG_ID','METER_ID'},'RightVariables',...
    {'JG_Phase'},'Type','left');
sx_UserCurrent=outerjoin(sx_UserCurrent,SXXW,'Keys',{'TG_ID','METER_ID','WIRING_MODE','PHASE_FLAG'},...
    'RightVariables',{'JG_Phase'},'Type','left');

UserVoltage=[dx_UserVoltage;sx_UserVoltage];
UserCurrent=[dx_UserCurrent;sx_UserCurrent];
UserVoltage.PHASE_FLAG=UserVoltage.JG_Phase;
UserVoltage(:,end)=[];
UserCurrent.PHASE_FLAG=UserCurrent.JG_Phase;
UserCurrent(:,end)=[];

clear dx_UserVoltage dx_UserCurrent sx_UserVoltage sx_UserCurrent
% ------------------------- 用户量测处理
UMV=stack(UserVoltage,8:103);
UMV.Properties.VariableNames(8:9)={'TIME','VOLT'};  % 电压数据纵向展开
m_UMV=grpstats(UMV,{'ORG_NO','TG_ID','METER_ID','PHASE_FLAG'},'mean',...
    'DataVars','VOLT');

UserCurrent{:,10:end}=UserCurrent.CT .* UserCurrent{:,10:end};
UMI=stack(UserCurrent,10:105);
UMI.Properties.VariableNames(10:11)={'TIME','CURRENT'};               % 用户电流数据纵向展开



%%

TQZK=intersect(UserCurrent(:,{'ORG_NO','TG_ID','METER_ID'}),...
    UserVoltage(:,{'ORG_NO','TG_ID','METER_ID'}),'rows');                                 % 电量量测数据表中不同的台区情况
TQZK=[TQZK,array2table(repmat([sd,ed,0,0,0],height(TQZK),1),'VariableNames',...
    {'StartDate','EndDate','GX','ZX','ZZK'})];        % 新增5列：开始日期，结束日期， 干线、支线、总阻抗
% 电压量测与功率量测中台区的交集
fun1=fittype('a*x1+b*x2','independent',{'x1','x2'},'dependent','Y',...
    'coefficients',{'a','b'});                                              % 户表有电流，采用二元线性回归
fun2=fittype('a*x1','independent',{'x1'},'dependent','Y',...
    'coefficients',{'a'});                                                  % 户表一直不存在电流，采用一元线性回归

tqs=intersect(TransCurrent(:,{'ORG_NO','TG_ID','METER_ID'}),...
    TransVoltage(:,{'ORG_NO','TG_ID','METER_ID'}),'rows');
MidResult=[];
for k1=1:height(tqs)
    TV=TME(:,logical([1;1;ismember(PBTG.TGID,tqs.TG_ID(k1))]));
    TV=[TV,table(categorical(strrep(cellstr(TV.TIME),'U','I')),'VariableNames',{'CTIME'})];
    TV=TV(:,[1:2,end,3:end-1]);
%     % 考虑三相不平衡中零序的影响，将总表的三相电压减去零序电压
%     TV{:,4:6}=abs(TV{:,4:6}.* [exp(0),exp(-1j*2*pi/3),exp(2j*pi/3)]-...
%         nansum(TV{:,4:6}.* [exp(0),exp(-1j*2*pi/3),exp(2j*pi/3)],2)./3);
    
    TranC=TCME(:,logical([1;1;ismember(transpose(extractAfter(...
        TCME.Properties.VariableNames(3:end),'x')),cellstr(string(tqs.METER_ID(k1))))]));
    
    
    
    TUV_A=DYPX(m_UMV(m_UMV.TG_ID==tqs.TG_ID(k1)&m_UMV.PHASE_FLAG==1,:));
    TUV_B=DYPX(m_UMV(m_UMV.TG_ID==tqs.TG_ID(k1)&m_UMV.PHASE_FLAG==2,:));
    TUV_C=DYPX(m_UMV(m_UMV.TG_ID==tqs.TG_ID(k1)&m_UMV.PHASE_FLAG==3,:));
    TransUserCurrent=innerjoin(UMI(UMI.TG_ID==tqs.TG_ID(k1),:),[TUV_A;TUV_B;TUV_C],'Keys',...
        {'ORG_NO','TG_ID','METER_ID','PHASE_FLAG'},'RightVariables',{'MV','PM'});
    if width(TV)<6
        TQZK{TQZK.TG_ID==tqs.TG_ID(k1),{'GX','ZX','ZZK'}}=-1.*ones(1.3);                                       % 配变缺相，所有用户回路阻抗定义为 “-1”
    else
        yhs=intersect(TransUserCurrent(:,{'ORG_NO','TG_ID','METER_ID'}),[TUV_A(:,{'ORG_NO','TG_ID',...
            'METER_ID'});TUV_B(:,{'ORG_NO','TG_ID','METER_ID'});TUV_C(:,{'ORG_NO','TG_ID','METER_ID'})]);  % 台区下的用户数
        for k2=1:height(yhs)
            dqyh=unique(TransUserCurrent(TransUserCurrent.METER_ID==yhs.METER_ID(k2),...
                {'ORG_NO','TG_ID','METER_ID','PHASE_FLAG','MV','PM'}));
            SingleUserVoltage=UMV(ismember(UMV.METER_ID,dqyh.METER_ID),:);
            if length(unique(dqyh.PHASE_FLAG))==1                     % 单相用户
                TransAndSingleUser=innerjoin(TV,SingleUserVoltage,'Keys',{'DATA_DATE','TIME'},...
                    'RightVariables',{'VOLT'});
                % 这里对配变电压与用户电压实现横向内联，保证后续的断面双方都有。
                FrontUserCurrent=TransUserCurrent((TransUserCurrent.PHASE_FLAG==dqyh.PHASE_FLAG)&...
                    (TransUserCurrent.PM>dqyh.PM),:);
                FrontUserCurrent=[FrontUserCurrent,array2table(FrontUserCurrent.CURRENT .* ...
                    dqyh.MV ./ FrontUserCurrent.MV,'VariableNames',{'FUCurrent'})];
                FrontUserCurrent=grpstats(FrontUserCurrent,{'ORG_NO','TG_ID','PHASE_FLAG',...
                    'DATA_DATE','TIME'},'nansum','DataVars',{'FUCurrent'});
                BackUserCurrent=TransUserCurrent((TransUserCurrent.PHASE_FLAG==dqyh.PHASE_FLAG)&...
                    (TransUserCurrent.PM<=dqyh.PM),:);
                BackUserCurrent=grpstats(BackUserCurrent,{'ORG_NO','TG_ID','PHASE_FLAG',...
                    'DATA_DATE','TIME'},'nansum','DataVars',{'CURRENT'});
                
                CUR=SelectJoin(FrontUserCurrent,BackUserCurrent,{'ORG_NO','TG_ID',...
                    'PHASE_FLAG','DATA_DATE','TIME'},{'nansum_FUCurrent'},{'nansum_CURRENT'});
                
                CUR=SelectJoin(CUR,TransUserCurrent(ismember(TransUserCurrent.METER_ID,dqyh.METER_ID)&...
                    ismember(TransUserCurrent.PHASE_FLAG,dqyh.PHASE_FLAG),:),{'ORG_NO','TG_ID',...
                    'PHASE_FLAG','DATA_DATE','TIME'},{'nansum_FUCurrent','nansum_CURRENT'},...
                    {'CURRENT'});
                CUR=fillmissing(CUR,'constant',0,'DataVariables',{'nansum_FUCurrent',...
                    'nansum_CURRENT','CURRENT'});
                TransAndSingleUser=innerjoin(TransAndSingleUser,CUR,'LeftKeys',{'DATA_DATE','CTIME'},...
                    'RightKeys',{'DATA_DATE','TIME'},'RightVariables',...
                    {'nansum_FUCurrent','nansum_CURRENT','CURRENT'});
                TransAndSingleUser=innerjoin(TransAndSingleUser,TranC,'Keys',...
                    {'DATA_DATE','CTIME'},'RightVariables',TranC.Properties.VariableNames(3:end));
                ys=[nansum(TransAndSingleUser{:,{'nansum_FUCurrent','nansum_CURRENT'}},2),...
                    TransAndSingleUser{:,'CURRENT'}];
                loc_c=ys(:,2)>0.5;                                      % 户表电流不低于一定的阈值
                
                if dqyh.PHASE_FLAG==1                    % A 相用户
                    deta_u=abs(TransAndSingleUser{:,4}-TransAndSingleUser.VOLT);
                elseif dqyh.PHASE_FLAG==2                % B 相用户
                    deta_u=abs(TransAndSingleUser{:,5}-TransAndSingleUser.VOLT);
                else                                     % C 相用户
                    deta_u=abs(TransAndSingleUser{:,6}-TransAndSingleUser.VOLT);
                end
                FTDA_y=RemoveMissing(ys(loc_c,:),deta_u(loc_c));
                FTDA_n=RemoveMissing(ys(~loc_c,:),deta_u(~loc_c));
                if sum(loc_c)>=0.6*height(TransAndSingleUser)       % 满足不为零的断面数量多于断面总数的30%
                    cfun1=fit(FTDA_y(:,1:end-1),FTDA_y(:,end),fun1,'Lower',[0,0],...
                        'Upper',[10,100],'StartPoint',[0,0]);
                    if cfun1.a>=0.005
                        zk=[cfun1.a,cfun1.b,nansum([cfun1.a,cfun1.b])];
                    else
                        cfun1=fit(FTDA_y(:,1),FTDA_y(:,end),fun2,'Lower',0,'Upper',10,'StartPoint',0);
                        zk=[cfun1.a,0,cfun1.a];
                    end
                else
                    cfun1=fit(FTDA_n(:,1),FTDA_n(:,end),fun2,...
                        'Lower',0,'Upper',10,'StartPoint',0);
                    zk=[cfun1.a,0,cfun1.a];                          % 支线阻抗强行赋0值
                end
            elseif length(unique(dqyh.PHASE_FLAG))==2              %  三相缺相用户
                TQZK{strcmp(TQZK.ORG_NO,dqyh.ORG_NO(1))&(TQZK.TG_ID==dqyh.TG_ID(1))&...
                    (TQZK.METER_ID==dqyh.METER_ID(1)),{'GX','ZX','ZZK'}}=-2.*ones(1,3);                             % 三相用户缺相，用户回路阻抗定义为“-2”
            else                                                                     % 三相用户
                SingleUserVoltage=unstack(SingleUserVoltage,'VOLT',{'PHASE_FLAG'},...
                    'NewDataVariableNames',{'UA','UB','UC'});
                TransAndSingleUser=innerjoin(TV,SingleUserVoltage,'Keys',{'DATA_DATE','TIME'},...
                    'RightVariables',{'UA','UB','UC'});
                FrontUserCurrent_A=TransUserCurrent((TransUserCurrent.PHASE_FLAG==1)&...
                    (TransUserCurrent.PM>dqyh.PM(dqyh.PHASE_FLAG==1)),:);
                FrontUserCurrent_B=TransUserCurrent((TransUserCurrent.PHASE_FLAG==2)&...
                    (TransUserCurrent.PM>dqyh.PM(dqyh.PHASE_FLAG==2)),:);
                FrontUserCurrent_C=TransUserCurrent((TransUserCurrent.PHASE_FLAG==3)&...
                    (TransUserCurrent.PM>dqyh.PM(dqyh.PHASE_FLAG==3)),:);
                
                FrontUserCurrent_A=[FrontUserCurrent_A,array2table(FrontUserCurrent_A.CURRENT .* ...
                    dqyh.MV(dqyh.PHASE_FLAG==1) ./ ...
                    FrontUserCurrent_A.MV,'VariableNames',{'FUCurrent'})];
                FrontUserCurrent_A=grpstats(FrontUserCurrent_A,{'ORG_NO','TG_ID','PHASE_FLAG',...
                    'DATA_DATE','TIME'},'nansum','DataVars',{'FUCurrent'});
                
                FrontUserCurrent_B=[FrontUserCurrent_B,array2table(FrontUserCurrent_B.CURRENT .* ...
                    dqyh.MV(dqyh.PHASE_FLAG==2) ./ ...
                    FrontUserCurrent_B.MV,'VariableNames',{'FUCurrent'})];
                FrontUserCurrent_B=grpstats(FrontUserCurrent_B,{'ORG_NO','TG_ID','PHASE_FLAG',...
                    'DATA_DATE','TIME'},'nansum','DataVars',{'FUCurrent'});
                
                FrontUserCurrent_C=[FrontUserCurrent_C,array2table(FrontUserCurrent_C.CURRENT .* ...
                    dqyh.MV(dqyh.PHASE_FLAG==3) ./ ...
                    FrontUserCurrent_C.MV,'VariableNames',{'FUCurrent'})];
                FrontUserCurrent_C=grpstats(FrontUserCurrent_C,{'ORG_NO','TG_ID','PHASE_FLAG',...
                    'DATA_DATE','TIME'},'nansum','DataVars',{'FUCurrent'});
                
                BackUserCurrent_A=TransUserCurrent((TransUserCurrent.PHASE_FLAG==1)&...
                    (TransUserCurrent.PM<=dqyh.PM(dqyh.PHASE_FLAG==1)),:);
                BackUserCurrent_A=grpstats(BackUserCurrent_A,{'ORG_NO','TG_ID','PHASE_FLAG',...
                    'DATA_DATE','TIME'},'nansum','DataVars',{'CURRENT'});
                BackUserCurrent_B=TransUserCurrent((TransUserCurrent.PHASE_FLAG==2)&...
                    (TransUserCurrent.PM<=dqyh.PM(dqyh.PHASE_FLAG==2)),:);
                BackUserCurrent_B=grpstats(BackUserCurrent_B,{'ORG_NO','TG_ID','PHASE_FLAG',...
                    'DATA_DATE','TIME'},'nansum','DataVars',{'CURRENT'});
                BackUserCurrent_C=TransUserCurrent((TransUserCurrent.PHASE_FLAG==3)&...
                    (TransUserCurrent.PM<=dqyh.PM(dqyh.PHASE_FLAG==3)),:);
                BackUserCurrent_C=grpstats(BackUserCurrent_C,{'ORG_NO','TG_ID','PHASE_FLAG',...
                    'DATA_DATE','TIME'},'nansum','DataVars',{'CURRENT'});
                
                CUR_A=SelectJoin(FrontUserCurrent_A,BackUserCurrent_A,{'ORG_NO','TG_ID',...
                    'PHASE_FLAG','DATA_DATE','TIME'},{'nansum_FUCurrent'},{'nansum_CURRENT'});
                
                CUR_A=SelectJoin(CUR_A,TransUserCurrent(ismember(TransUserCurrent.METER_ID,...
                    dqyh.METER_ID)&TransUserCurrent.PHASE_FLAG==1,:),{'ORG_NO','TG_ID',...
                    'PHASE_FLAG','DATA_DATE','TIME'},{'nansum_FUCurrent','nansum_CURRENT'},...
                    {'CURRENT'});
                
                CUR_A=fillmissing(CUR_A,'constant',0,'DataVariables',{'nansum_FUCurrent',...
                    'nansum_CURRENT','CURRENT'});
                
                CUR_B=SelectJoin(FrontUserCurrent_B,BackUserCurrent_B,{'ORG_NO','TG_ID',...
                    'PHASE_FLAG','DATA_DATE','TIME'},{'nansum_FUCurrent'},{'nansum_CURRENT'});
                
                CUR_B=SelectJoin(CUR_B,TransUserCurrent(ismember(TransUserCurrent.METER_ID,...
                    dqyh.METER_ID)&TransUserCurrent.PHASE_FLAG==2,:),{'ORG_NO','TG_ID',...
                    'PHASE_FLAG','DATA_DATE','TIME'},{'nansum_FUCurrent','nansum_CURRENT'},...
                    {'CURRENT'});
                
                CUR_B=fillmissing(CUR_B,'constant',0,'DataVariables',{'nansum_FUCurrent',...
                    'nansum_CURRENT','CURRENT'});
                
                CUR_C=SelectJoin(FrontUserCurrent_C,BackUserCurrent_C,{'ORG_NO','TG_ID',...
                    'PHASE_FLAG','DATA_DATE','TIME'},{'nansum_FUCurrent'},{'nansum_CURRENT'});
                
                CUR_C=SelectJoin(CUR_C,TransUserCurrent(ismember(TransUserCurrent.METER_ID,...
                    dqyh.METER_ID)&TransUserCurrent.PHASE_FLAG==3,:),{'ORG_NO','TG_ID',...
                    'PHASE_FLAG','DATA_DATE','TIME'},{'nansum_FUCurrent','nansum_CURRENT'},...
                    {'CURRENT'});
                
                CUR_C=fillmissing(CUR_C,'constant',0,'DataVariables',{'nansum_FUCurrent',...
                    'nansum_CURRENT','CURRENT'});
                TransAndSingleUser=innerjoin(TransAndSingleUser,CUR_A,'LeftKeys',{'DATA_DATE','CTIME'},...
                    'RightKeys',{'DATA_DATE','TIME'},'RightVariables',{'nansum_FUCurrent',...
                    'nansum_CURRENT','CURRENT'});
                TransAndSingleUser.Properties.VariableNames((end-2):end)={'nJQCA','nCA','CA'};
                TransAndSingleUser=innerjoin(TransAndSingleUser,CUR_B,'LeftKeys',{'DATA_DATE','CTIME'},...
                    'RightKeys',{'DATA_DATE','TIME'},'RightVariables',{'nansum_FUCurrent',...
                    'nansum_CURRENT','CURRENT'});
                TransAndSingleUser.Properties.VariableNames((end-2):end)={'nJQCB','nCB','CB'};
                TransAndSingleUser=innerjoin(TransAndSingleUser,CUR_C,'LeftKeys',{'DATA_DATE','CTIME'},...
                    'RightKeys',{'DATA_DATE','TIME'},'RightVariables',{'nansum_FUCurrent',...
                    'nansum_CURRENT','CURRENT'});
                TransAndSingleUser.Properties.VariableNames((end-2):end)={'nJQCC','nCC','CC'};
                TransAndSingleUser=innerjoin(TransAndSingleUser,TranC,'Keys',...
                    {'DATA_DATE','CTIME'},'RightVariables',TranC.Properties.VariableNames(3:end));
                
                ys_A=[nansum(TransAndSingleUser{:,{'nJQCA','nCA'}},2),TransAndSingleUser{:,'CA'}];
                ys_B=[nansum(TransAndSingleUser{:,{'nJQCB','nCB'}},2),TransAndSingleUser{:,'CB'}];
                ys_C=[nansum(TransAndSingleUser{:,{'nJQCC','nCC'}},2),TransAndSingleUser{:,'CC'}];
                ys=(ys_A+ys_B+ys_C)./3;
                deta_u=nanmean(abs(TransAndSingleUser{:,4:6}-TransAndSingleUser{:,{'UA','UB','UC'}}),2);
                
                loc_c=ys(:,2)>0.5;                                          % 户表电流不低于一定的阈值 ！
                FTDA_y=RemoveMissing(ys(loc_c,:),deta_u(loc_c));
                FTDA_n=RemoveMissing(ys(~loc_c,:),deta_u(~loc_c));
                
                if sum(loc_c)>=0.6*height(TransAndSingleUser)       % 满足不为零的断面数量多于断面总数的30%
                    cfun1=fit(FTDA_y(:,1:end-1),FTDA_y(:,end),fun1,'Lower',[0,0],...
                        'Upper',[10,100],'StartPoint',[0,0]);
                    if cfun1.a>=0.005
                        zk=[cfun1.a,cfun1.b,nansum([cfun1.a,cfun1.b])];
                    else
                        cfun1=fit(FTDA_y(:,1),FTDA_y(:,end),fun2,'Lower',0,'Upper',10,'StartPoint',0);
                        zk=[cfun1.a,0,cfun1.a];
                    end
                else
                    cfun1=fit(FTDA_n(:,1),FTDA_n(:,end),fun2,...
                        'Lower',0,'Upper',10,'StartPoint',0);
                    zk=[cfun1.a,0,cfun1.a];                          % 支线阻抗强行赋0值
                end
                
            end
            TQZK{strcmp(TQZK.ORG_NO,dqyh.ORG_NO(1))&(TQZK.TG_ID==dqyh.TG_ID(1))&...
                (TQZK.METER_ID==dqyh.METER_ID(1)),{'GX','ZX','ZZK'}}=zk;
            aaa=[repmat(dqyh(1,[1:3,5,6]),length(ys),1),...
                TransAndSingleUser(:,(end-2):end),array2table([transpose(1:length(ys)),...
                deta_u,ys],'VariableNames',{'Sequence','VoltageDeviation','GX_Current',...
                'ZX_Current'})];
            aaa.Properties.VariableNames(6:8)={'IA','IB','IC'};
            MidResult=[MidResult;aaa];
            clear aaa;
        end
        
    end
    
end

%%
% % ------------------------------Here is the internal funciton definition

    function px=DYPX(tba)
        % % ------- 按照升序排列用户电压，剔除
        Volt=tba{:,6};
        [~,b]=sortrows(Volt,'ascend');
        c=sortrows([transpose(1:length(b)),b],2,'ascend');
        tba=[tba,array2table(c(:,1),'VariableNames',{'PM'})];        % 电压均值，排名
        tba.Properties.VariableNames(6)={'MV'};
        px=sortrows(tba,{'MV','PM'});
        
    end

    function fd = FEEL(AA,cn)
        %  FEEL = Find Equal Element Location
        ue=unique(AA);
        fd=zeros(length(AA),1);
        for k=1:length(ue)
            
            aa=find(strcmp(AA,ue(k))==1);
            aa=sort(aa,'ascend');
            fd((cn*(k-1)+1):(cn*k),1)=aa;
            
        end
    end

    function RM=RemoveMissing(R1,R2)
        % 剔除空值数据
        ycc=logical(nansum(isnan(R1),2))|isnan(R2);
        R1(ycc,:)=[];
        R2(ycc,:)=[];
        RM=[R1,R2];
        
    end

    
    function sj=SelectJoin(S1,S2,keys,lv,rv)
    % 选择性关联，防止待关联的两张表中出现某一张表为空的情况
    % S1，表1；S2，表2；keys，关联字段原包，行向量；
    % lv，左侧表的数据变量，rv，右侧表的数据变量
        
        if height(S1)==0
            sj=[S2(:,keys),array2table(zeros(height(S2),length(lv)),'VariableNames',lv),S2(:,rv)];
        elseif height(S2)==0
            sj=[S1(:,[keys,lv]),array2table(zeros(height(S1),length(rv)),'VariableNames',rv)];
        else
            sj=outerjoin(S1,S2,'Keys',keys,'RightVariables',rv,'Type','full');
        end
    end

    function EVUE=EliminateVoltageUnbalanceEffect(tbl_tv,dn)
        % 剔除电压参数的零序部分
        evue=zeros(height(tbl_tv),width(tbl_tv)-2);
        RotationFactor=[exp(0),exp(-1j*2*pi/3),exp(2j*pi/3)];   % 旋转因子
        for af=1:(width(tbl_tv)-2)/3
            evue(:,(3*af-2):3*af)=abs(tbl_tv{:,3*af:(3*af+2)}.* RotationFactor-...
                nansum(tbl_tv{:,3*af:(3*af+2)}.* RotationFactor,2)./3);
        end
        if dn>0
            tbl_tv{:,3:end}=evue;
        end
        
        EVUE=tbl_tv;
        
    end
    
    function sxxg=TPS(MX,MY)
        XX=[1,2,3;1,3,2;2,1,3;2,3,1;3,1,2;3,2,1];
        
        sxxg=nansum([diag(corr(MX(:,XX(1,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(2,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(3,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(4,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(5,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(6,:)),MY,'Type','Pearson','Rows','pairwise'))],1);
        
    end


end


