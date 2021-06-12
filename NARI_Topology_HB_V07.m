% function [JGResult,nu]= NARI_Topology_HB_V07(sd,ed,qy,nearby_tg)
% NARI_VU_TQTP = 石家庄数据计算
% 按相邻台区计算；
% ——相对V03 ，升级成按日计算，统计分布概率，以概率最大作为最终结果。
% ——相对于V04，升级户表数据处理、筛选规则，判定用户数据出现电压数据异常时放弃该用户。
% ——新增配变负载率计算，当前要求负载率低于 2% 默认该台区及其档案用户不参与辨识算法；
% ——相对于V05，升级计算结果带出计算的相似度，增加采集ID列，增加每天的相似度计算结果输出，增加异常数据
% 剔除原因描述
% ——相较于V06，该版本为测试方法 采用"Tanimoto"相似性匹配方法



%%
% % -----------------------库连接，取数据
% Conn_NARI=database('sea1','hebei','sea3000', 'oracle.jdbc.driver.OracleDriver', ...
%     'jdbc:oracle:thin:@192.168.176.51:1521:');
%

sd=20200801;
ed=20200831;
qy='134010902';
% nearby_tg=TK;
nearby_tg='(1646245,1680747,1646246)';   % 大同所 134021306；回舍所134010902

% -----------------------------------
SDate=['''',num2str(sd),''''];
EDate=['''',num2str(ed),''''];
QY=['''',qy,''''];


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
    ' FROM e_mp_vol_curve vc ',...
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
    ' FROM e_mp_vol_curve vc ',...
    ' INNER JOIN e_data_mp edp ON vc.id=edp.id ',...
    ' INNER JOIN G_TG TG ON EDP.TG_ID=TG.TG_ID ',...
    ' WHERE TG.PUB_PRIV_FLAG = ','''01''',' AND vc.data_date>=TO_DATE(',SDate,',','''yyyymmdd''',') ',...
    ' AND vc.data_date<=TO_DATE(',EDate,',','''yyyymmdd''',') AND edp.cons_sort= ','''06''',...
    ' AND vc.phase_flag in(1,2,3) and tg.org_no like ',QY ,...
    ' AND TG.TG_ID IN ',nearby_tg,...
    ' ORDER BY tg.org_no,tg.tg_id,edp.cons_sort,vc.data_date,vc.phase_flag '];   % 配变电压

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

sq_tc=['SELECT TG.TG_ID,TG.TG_CAP,TG.ORG_NO,TO_CHAR(CC.DATA_DATE,', '''yyyymmdd''',') AS DATA_DATE,',...
    ' EDP.METER_ID,EDP.CONS_SORT,EDP.WIRING_MODE,CC.CT,CC.MARK,CC.PHASE_FLAG, ',...
    ' CC.I1,	CC.I5,	CC.I9,	CC.I13,	CC.I17,	CC.I21,	CC.I25,	CC.I29,',...
	' CC.I33,	CC.I37,	CC.I41,	CC.I45,	CC.I49,	CC.I53,	CC.I57,	CC.I61,',...
	' CC.I65,	CC.I69,	CC.I73,	CC.I77,	CC.I81,	CC.I85,	CC.I89,	CC.I93 ',...
    ' FROM E_MP_CUR_CURVE cc INNER JOIN e_data_mp edp ON cc.id=edp.id ',...
    ' INNER JOIN G_TG TG ON EDP.TG_ID=TG.TG_ID WHERE TG.PUB_PRIV_FLAG = ','''01''',...
    ' AND CC.DATA_DATE >= TO_DATE(',SDate,',', '''yyyymmdd''',') ',...
    ' AND cc.data_date<=TO_DATE(',EDate,',','''yyyymmdd''',')',...
    ' AND EDP.CONS_SORT = ','''06''','AND CC.PHASE_FLAG IN (1, 2, 3) ',...
    ' AND TG.ORG_NO LIKE ',QY,' AND TG.TG_ID IN ',nearby_tg,...
    ' ORDER BY TG.ORG_NO, TG.TG_ID, EDP.CONS_SORT, CC.DATA_DATE, CC.PHASE_FLAG'];

sq_topo=['SELECT tg.org_no,tg.tg_id,tg.tg_cap,edm.meter_id,edm.id,edm.fc_gc_flag FROM e_data_mp edm ',...
    ' INNER JOIN g_tg tg ON edm.tg_id=tg.tg_id ',...
    ' WHERE tg.org_no LIKE ',QY,' AND tg.pub_priv_flag = ','''01''' ,...
    ' AND tg.run_status_code = ','''01''','AND TG.TG_ID IN ',nearby_tg,...
    ' ORDER BY tg.tg_id,edm.meter_id'];

%
UserVoltage=select(Conn_NARI,sq_uv);
TransVoltage=select(Conn_NARI,sq_tv);
TransPower=select(Conn_NARI,sq_tp);
% TransCurrent=select(Conn_NARI,sq_tc);
TGTopo=select(Conn_NARI,sq_topo);
close(Conn_NARI);
clear Conn_NARI sq_t sq_v;


%%
%%
% % -------------------- 数据处理规则
% 单相用户电压全为0剔除
nu1=UserVoltage(nansum(UserVoltage{:,8:end},2)==0,{'ORG_NO','TG_ID','METER_ID','DATA_DATE'});
UserVoltage(strcmp(UserVoltage.WIRING_MODE,'1')&(nansum(UserVoltage{:,8:end},2)==0),:)=[];
nu1=UserVoltage(nansum(UserVoltage{:,8:end},2)==0,{'ORG_NO','TG_ID','METER_ID','DATA_DATE'});
% --2-- 户表高电压、低电压异常剔除
abn_uv=[nanmin(UserVoltage{:,8:end},[],2),nanmax(UserVoltage{:,8:end},[],2)];
nu2=UserVoltage((abn_uv(:,1)<190&abn_uv(:,2)>250),{'ORG_NO','TG_ID','METER_ID','DATA_DATE'});
UserVoltage((ismember(UserVoltage.ORG_NO,nu2.ORG_NO)&ismember(UserVoltage.TG_ID,nu2.TG_ID)&...
    ismember(UserVoltage.METER_ID,nu2.METER_ID)&ismember(UserVoltage.DATA_DATE,nu2.DATA_DATE)),:)=[];
nu=union(nu1,nu2);   % 数据异常的用户集合


% ---- 低负载配变及其下辖用户剔除；
% ---- 低负载配变及其下辖用户剔除；
% --(1) 通过功率剔除
LoadLowerLimit=2;                                                                        % 负载率下限
TransPower{:,10:105}=TransPower.T_FACTOR.*abs(TransPower{:,10:105});
sum_transpower=grpstats(TransPower,{'ORG_NO','TG_ID','METER_ID','TG_CAP',...
    'DATA_DATE'},'nansum','DataVars',TransPower.Properties.VariableNames(10:105));
LoadRatio=100.*nanmean(sum_transpower{:,7:102},2)./sum_transpower.TG_CAP;
AbnormalTrans=[sum_transpower(LoadRatio<=LoadLowerLimit,1:5),...
    array2table(LoadRatio(LoadRatio<=LoadLowerLimit),...
    'VariableNames',{'MeanLoadRate'})];
TransVoltage(ismember(TransVoltage.ORG_NO,AbnormalTrans.ORG_NO)&...
    ismember(TransVoltage.TG_ID,AbnormalTrans.TG_ID)&...
    ismember(TransVoltage.DATA_DATE,AbnormalTrans.DATA_DATE),:)=[];          % 低负载配备电压数据剔除
UserVoltage(ismember(UserVoltage.ORG_NO,AbnormalTrans.ORG_NO)&...
    ismember(UserVoltage.TG_ID,AbnormalTrans.TG_ID)&...
    ismember(UserVoltage.DATA_DATE,AbnormalTrans.DATA_DATE),:)=[];            % 低负载配变档案下的用户电压数据剔除

% % --（2）-通过电流剔除
% 
% STC=grpstats(TransCurrent,{'TG_ID','TG_CAP','ORG_NO','DATA_DATE','METER_ID','CT'},...
%     'nansum','DataVars',TransCurrent.Properties.VariableNames(11:end));
% 
% DailyLoadRate=[STC(:,{'TG_ID','TG_CAP','ORG_NO','DATA_DATE','METER_ID','CT'}),...
%     array2table(0.1.*235.*nanmean(STC{:,8:end},2).*STC.CT./STC.TG_CAP,'VariableNames',{'LoadRate'})];
% 
% TransVoltage=TransVoltage(ismember(TransVoltage.TG_ID,DailyLoadRate.TG_ID(DailyLoadRate.LoadRate>2)),:);
% UserVoltage=UserVoltage(ismember(UserVoltage.TG_ID,DailyLoadRate.TG_ID(DailyLoadRate.LoadRate>2)),:);



%%
% % -------------------------------弱特征匹配
TM=stack(TransVoltage,8:103);                     % 行转列
TM.Properties.VariableNames(8:9)={'TIME','VOLT'};
TM=unstack(TM,{'VOLT'},{'PHASE_FLAG'},'NewDataVariableNames',{'UA','UB','UC'});
% TM.Properties.VariableNames(7:9)={'UA','UB','UC'};
TM=unstack(TM,{'UA','UB','UC'},'METER_ID','GroupingVariables',{'DATA_DATE','TIME'});             % 配变量测方式排布，案列展开，日期+时刻

TNM=transpose(TM.Properties.VariableNames(3:end));
TPnm=extractBefore(TNM,'_');
TTnm=extractAfter(TNM,'x');
SX=FEEL(TTnm);
TME=TM(:,[1:2,SX'+2]);
TMIntegrality=array2table([str2double(TTnm(SX)),transpose(100.*(1-sum(isnan(TME{:,3:end})|...
    (TME{:,3:end}==0),1)./height(TME)))],'VariableNames',{'TransMeterID','TransDataIntegrality'});
clear TM TNM;

PBTG=table(str2double(TTnm(SX)),TPnm(SX),'VariableNames',{'METER_ID','PHASE'});     % 配变的台区属性；
[PBTG,ip,~]=outerjoin(PBTG,unique(TransVoltage(:,{'TG_ID','METER_ID'})),'Keys','METER_ID',...
    'RightVariables','TG_ID','type','left');
ipp=sortrows([transpose(1:length(ip)),ip],2,'ascend');
PBTG=PBTG(ipp(:,1),:);
PBTG.Properties.VariableNames(1:3)={'TransMeterID','PHASE','TGID'};                % 建档数据归集
TMIntegrality=outerjoin(TMIntegrality,PBTG(strcmp(PBTG.PHASE,'UA'),:),'Keys','TransMeterID',...
    'RightVariables','TGID','Type','left');
TMIntegrality=TMIntegrality(:,[3,1,2]);
TMIntegrality=grpstats(TMIntegrality,'TGID','mean','DataVars','TransDataIntegrality');

clear TPnm TTnm;

DA=fillmissing(TME{:,3:end},'movmedian',12,1);
loc=DA==0;
DA(loc)=nan;
DA=fillmissing(DA,'movmedian',12,1,'EndValues','nearest');
TME{:,3:end}=DA;
clear TransMeasure loc DA;

% % --------计算每个台区的零序矩阵；
ro=exp(1j*2*pi/3);   % 旋转因子；
zsc=zeros(height(TME),(width(TME)-2));    % 记录零序分量模值大小；zero sequence component
for w=1:(width(TME)-2)/3
    alfa=abs(nansum(TME{:,(3*w):(3*w+2)}.*[1,ro^2,ro],2))./3;
    zsc(:,(3*w-2):(3*w))=repmat(alfa,1,3);
end
zsc=[TME(:,1:2),array2table(zsc,'VariableNames',TME.Properties.VariableNames(3:end))];



% ------------------------- 用户量测处理
UM=stack(UserVoltage,8:103);
UM.Properties.VariableNames(8:9)={'TIME','VOLT'};

DPU=unique(UM(:,{'ORG_NO','TG_ID','METER_ID','WIRING_MODE','PHASE_FLAG'}));
DXUser=DPU(strcmp(DPU.WIRING_MODE,'1'),{'ORG_NO','TG_ID','METER_ID','WIRING_MODE','PHASE_FLAG'});
SXUser=unique(DPU(~strcmp(DPU.WIRING_MODE,'1'),{'ORG_NO','TG_ID','METER_ID','WIRING_MODE'}));
SXUser=[SXUser,array2table(repmat(123,height(SXUser),1),'VariableNames',{'PHASE_FLAG'})];


DXU=UM(strcmp(UM.WIRING_MODE,'1'),:);
DXUM=unstack(DXU,'VOLT','METER_ID','GroupingVariables',{'DATA_DATE','TIME'});          % 单相用户量测表
dx_userid=str2double(extractAfter(transpose(DXUM.Properties.VariableNames(3:end)),'x')); % 从第三列开始为用户数据
DXUIntegrality=array2table([dx_userid,transpose(100.*(1-sum(isnan(DXUM{:,3:end})|...
    (DXUM{:,3:end}==0),1)./height(DXUM)))],'VariableNames',{'METER_ID','DXUserDataIntegrality'});
DA2=fillmissing(DXUM{:,3:end},'movmedian',12,1);
loc2=DA2==0;
DA2(loc2)=nan;
DA2=fillmissing(DA2,'movmedian',12,1,'EndValues','nearest');
DXUM{:,3:end}=DA2;
clear DA2 loc2;

SXU=UM(~strcmp(UM.WIRING_MODE,'1'),:);
if length(unique(SXU.PHASE_FLAG))==1
    SXUM=unstack(SXU,{'VOLT'},{'PHASE_FLAG'},'NewDataVariableNames',{'UA'});
    SXUM=[SXUM,array2table(zeros(height(SXUM),2),'VariableNames',{'UB','UC'})];
else
    SXUM=unstack(SXU,{'VOLT'},{'PHASE_FLAG'},'NewDataVariableNames',{'UA','UB','UC'});
end
SXUM=unstack(SXUM,{'UA','UB','UC'},'METER_ID','GroupingVariables',{'DATA_DATE','TIME'});
snm=transpose(SXUM.Properties.VariableNames(3:end));
sxyh=extractAfter(snm,'x');
sxsx=FEEL(sxyh);
SXUM=SXUM(:,[1:2,sxsx'+2]);
sx_userid=str2double(extractAfter(transpose(SXUM.Properties.VariableNames(3:end)),'x'));          % 从第4列开始为用户数据
SXUIntegrality=array2table([sx_userid,transpose(100.*(1-nansum(isnan(SXUM{:,3:end})|...
    (SXUM{:,3:end}==0),1)./height(SXUM)))],'VariableNames',{'METER_ID','SXUserDataIntegrality'});
SXUIntegrality=grpstats(SXUIntegrality,{'METER_ID'},'mean');
clear UM snm sxyh sxsx;

DA3=fillmissing(SXUM{:,3:end},'movmedian',12,1);
loc3=DA3==0;
DA3(loc3)=nan;
DA3=fillmissing(DA3,'movmedian',12,1,'EndValues','nearest');
SXUM{:,3:end}=DA3;
clear DA3 loc3;


%% 
% % ------------ 神经网络测试
% jdxx=arrya2table(str2double(transpose(extractAfter(DXUM.Properties.VariableNames(3:end),'x'))),...
%     'VariableNames',{'METER_ID'});
% jdxx=outerjoin(jdxx,DXUser,'Keys',{'METER_ID'},'RightVariables',{'TG_ID'},'Type','left');
% 
% pnet = patternnet(96);
% pnet = train(pnet,DXUM{:,3:end},jdxx.TG_ID);








%%
% % ============================================ 多日数据拼接判定。
% % ------------------------------------------         单相用户分析
% dxCU=zeros(height(DXUser),width(TME)-2);                             % 单相用户与配变整体相关性计算
% dxCDU=zeros(height(DXUser),width(TME)-2);
% % DX_MID_RE=[];
% 
% for h1=1:height(DXUser)
%     XD=outerjoin(DXUM(:,[1,2,find(dx_userid==DXUser.METER_ID(h1))+2]),TME,'Keys',{'DATA_DATE','TIME'},...
%         'RightVariables',TME.Properties.VariableNames(3:end),'Type','right');
%     XD=fillmissing(XD,'movmedian',12,'DataVariables',XD.Properties.VariableNames(3:end));
%     tc=transpose(linspace(1,height(XD),15*height(XD)));
%     XDD=[];
%     for se=1:(width(XD)-2)
%         XDD(:,se)=interp1(transpose(1:height(XD)),XD{:,se+2},tc,'makima');
%     end
%     
%     uxg=corr(XDD,'rows','pairwise','type','Pearson');
% %     uxg=corr(XD{:,3:end},'rows','pairwise','type','Pearson');
% %     duxg=corr(diff(XD{:,3:end},1,1),'rows','pairwise','Type','Pearson');
%     dxCU(h1,:)=uxg(1,2:end);
% %     dxCDU(h1,:)=duxg(1,2:end);
%     clear XD uxg duxg;
% end  
% 
% [zdx_d,mCU]=max(dxCU,[],2);
% JG_DXU=[DXUser,PBTG(mCU,[1,3,2]),array2table(zdx_d,'VariableNames',{'MCorr'})];
% 
% 
% 
% 
% 
% 
% 
% % % ------------------------------------------------ 三相用户分析
% sxCU=zeros(height(SXUser),length(SX)/3,6);                          % 三相用户与配变三相的6种组合相关性计算
% sxCDU=zeros(height(SXUser),length(SX)/3,6);
% % SX_MID_RE=[];
% for h2=1:height(SXUser)
%     %     sxum=unstack(UM((UM.TG_ID==SXUser.TG_ID(h2))&(UM.METER_ID==SXUser.METER_ID(h2)),:),{'VOLT'},...
%     %         {'PHASE_FLAG'},'NewDataVariableNames',{'UA','UB','UC'});
%     sxum=SXUM(:,[1:2,(find(sx_userid==SXUser.METER_ID(h2))+2)']);
%     for h3=1:length(SX)/3
%         smm=outerjoin(sxum,TME(:,[1:2,3*h3:(3*h3+2)]),'Keys',{'DATA_DATE','TIME'},...
%             'RightVariables',TME.Properties.VariableNames(3*h3:(3*h3+2)),'Type','right');
%         smm=fillmissing(smm,'movmedian',12,'DataVariables',smm.Properties.VariableNames(3:end));
%         tc=transpose(linspace(1,height(smm),15*height(smm)));
%         flsm=[interp1(transpose(1:height(smm)),smm{:,3},tc),...
%             interp1(transpose(1:height(smm)),smm{:,4},tc),...
%             interp1(transpose(1:height(smm)),smm{:,5},tc),...
%             interp1(transpose(1:height(smm)),smm{:,6},tc),...
%             interp1(transpose(1:height(smm)),smm{:,7},tc),...
%             interp1(transpose(1:height(smm)),smm{:,8},tc)];
%         mm=TPS(flsm(:,1:3),flsm(:,4:6));
% %         mm=TPS(smm{:,3:5},smm{:,6:8});
% %         mn=TPS(diff(smm{:,3:5},1,1),diff(smm{:,6:8},1,1));
%         sxCU(h2,h3,:)=mm;
% %         sxCDU(h2,h3,:)=mn;
%         clear smm mm mn;
%     end
%     clear sxum;
% end
% 
% [zdxx,loc_zdxx]=max(sxCU,[],3);
% [zdx_s,loc_zdtg]=max(zdxx,[],2);                 % 最大台区相似度数值，最大相似度对应台区的数值；
% atg=PBTG(strcmp(PBTG.PHASE,'UA'),:);
% sxxx=diag(loc_zdxx(:,loc_zdtg));
% JG_SXU=[SXUser,atg(loc_zdtg,[1,3]),array2table([sxxx,zdx_s],'VariableNames',{'PhaseSequence','MCorr'})];

% %

%% 

% --------==============================================================================================        按日分析
% dxCU=zeros(height(DXUser),width(TME)-2);                             % 单相用户与配变整体相关性计算
% dxCDU=zeros(height(DXUser),width(TME)-2);
% DX_MID_RE=[];
% for dd=0:(datenum(num2str(ed),'yyyymmdd')-datenum(num2str(sd),'yyyymmdd'))
%     jsrq=datestr(datestr(datenum(num2str(sd),'yyyymmdd')+dd),'yyyymmdd');    % 计算的日期
%     for h1=1:height(DXUser)
%         XD=outerjoin(DXUM(strcmp(DXUM.DATA_DATE,jsrq),[1,2,find(dx_userid==DXUser.METER_ID(h1))+2]),...
%             TME(strcmp(TME.DATA_DATE,jsrq),:),'Keys',{'DATA_DATE','TIME'},'RightVariables',...
%             TME.Properties.VariableNames(3:end),'Type','right');
%         XD=fillmissing(XD,'movmedian',12,'DataVariables',XD.Properties.VariableNames(3:end),...
%             'EndValues','nearest');
%         XD=fillmissing(XD,'constant',0,'DataVariables',XD.Properties.VariableNames(3:end));
%         tc=transpose(linspace(1,height(XD),15*height(XD)));
%         for se=1:(width(XD)-2)
%             XDD(:,se)=interp1(transpose(1:height(XD)),XD{:,se+2},tc,'makima');
%         end
%         alter_xdd=XDD(:,1);
%         for dt=1:15
%             alter_xdd=[circshift(XDD(:,1),-dt,1),alter_xdd,circshift(XDD(:,1),dt,1)];
%         end
%         uxg=corr([alter_xdd,XDD(:,2:end)],'rows','pairwise','type','Pearson');
%         uxg=max(uxg(1:31,32:end),[],1);
% %         uxg=corr(XD{:,3:end},'rows','pairwise','type','Pearson');
% %         duxg=corr(diff(XD{:,3:end},1,1),'rows','pairwise','Type','Pearson');
% %         dxCU(h1,:)=uxg(1,2:end);
%         dxCU(h1,:)=uxg;
% %         dxCDU(h1,:)=duxg(1,2:end);
%         clear XD uxg duxg;
%     end
% %     [mcdx,loc_mcdx]=max(dxCU,[],2);
% %     DX_MID_RE=[DX_MID_RE;[DXUser,cell2table(cellstr(repmat(jsrq,height(DXUser),1)),'VariableNames',...
% %         {'DATA_DATE'}),PBTG(loc_mcdx,[1,3,2]),array2table(mcdx,'VariableNames',{'DailyCorr'})]];
% %     mx_dd(:,:,dd+1)=double(dxCU==max(dxCU,[],2));             % 概率模型计算
%     mx_dd(:,:,dd+1)=dxCU;                                       % 累加模型计算
%     
% end
% [zdx_d,mCU]=max(nanmean(mx_dd,3),[],2);
% JG_DXU=[DXUser,PBTG(mCU,[1,3,2]),array2table(zdx_d,'VariableNames',{'MCorr'})];
% 
% 
% 
% % % ------------------------------------------------ 三相用户分析
% sxCU=zeros(height(SXUser),length(SX)/3,6);                          % 三相用户与配变三相的6种组合相关性计算
% sxCDU=zeros(height(SXUser),length(SX)/3,6);
% SX_MID_RE=[];
% for ds=0:(datenum(num2str(ed),'yyyymmdd')-datenum(num2str(sd),'yyyymmdd'))
%     jsrq=datestr(datestr(datenum(num2str(sd),'yyyymmdd')+ds),'yyyymmdd');    % 计算的日期
%     for h2=1:height(SXUser)
%         sxum=SXUM(strcmp(SXUM.DATA_DATE,jsrq),[1:2,(find(sx_userid==SXUser.METER_ID(h2))+2)']);
%         for h3=1:length(SX)/3
%             smm=outerjoin(sxum,TME(strcmp(TME.DATA_DATE,jsrq),[1:2,3*h3:(3*h3+2)]),'Keys',...
%                 {'DATA_DATE','TIME'},'RightVariables',TME.Properties.VariableNames(3*h3:(3*h3+2)),...
%                 'Type','right');
%             smm=fillmissing(smm,'movmedian',12,'DataVariables',smm.Properties.VariableNames(3:end),...
%                 'EndValues','nearest');
%             smm=fillmissing(smm,'constant',0,'DataVariables',smm.Properties.VariableNames(3:end));
%             tc=transpose(linspace(1,height(smm),15*height(smm)));
%             flsm=[interp1(transpose(1:height(smm)),smm{:,3},tc),...
%                 interp1(transpose(1:height(smm)),smm{:,4},tc),...
%                 interp1(transpose(1:height(smm)),smm{:,5},tc),...
%                 interp1(transpose(1:height(smm)),smm{:,6},tc),...
%                 interp1(transpose(1:height(smm)),smm{:,7},tc),...
%                 interp1(transpose(1:height(smm)),smm{:,8},tc)];
%             DT=transpose(-15:1:15);
%             for dt=1:length(DT)
%                 mm(dt,:)=TPS(circshift(flsm(:,1:3),DT(dt),1),flsm(:,4:6))./3;
%             end
% %             mm=TPS(flsm(:,1:3),flsm(:,4:6))./3;
%             mm=max(mm,[],1);
% %             mm=TPS(smm{:,3:5},smm{:,6:8})./3;
%             %             mn=TPS(diff(smm{:,3:5},1,1),diff(smm{:,6:8},1,1));
%             sxCU(h2,h3,:)=mm;
%             %             sxCDU(h2,h3,:)=mn;
%             clear smm mm;
%         end
%         clear sxum;
%     end
%     ls_pbtg=PBTG(strcmp(PBTG.PHASE,'UA'),:);
%     [mcsx,loc_mcsx]=max(sxCU,[],3);
%     [mctg,loc_mctg]=max(mcsx,[],2);
%     dsxxx=diag(loc_mcsx(:,loc_mctg));
% %     SX_MID_RE=[SX_MID_RE;[SXUser,cell2table(cellstr(repmat(jsrq,height(SXUser),1)),'VariableNames',...
% %         {'DATA_DATE'}),ls_pbtg(loc_mctg,[1,3]),array2table([dsxxx,mctg],'VariableNames',...
% %         {'PhaseSequence','DailyCorr'})]];
%     %     mx_ss(:,:,:,ds+1)=double(sxCU==max(sxCU,[],3));         % 多日最大概率模型计算
%     mx_ss(:,:,:,ds+1)=sxCU;                                   % 多日最大累加模型计算
% end
% 
% [zdxx,loc_zdxx]=max(nanmean(mx_ss,4),[],3);
% [zdx_s,loc_zdtg]=max(zdxx,[],2);                 % 最大台区相似度数值，最大相似度对应台区的数值；
% atg=PBTG(strcmp(PBTG.PHASE,'UA'),:);
% sxxx=diag(loc_zdxx(:,loc_zdtg));
% JG_SXU=[SXUser,atg(loc_zdtg,[1,3]),array2table([sxxx,zdx_s],'VariableNames',{'PhaseSequence','MCorr'})];

%% 

dxCU=zeros(height(DXUser),width(TME)-2);                             % 单相用户与配变整体相关性计算
dxCDU=zeros(height(DXUser),width(TME)-2);
DX_MID_RE=[];
for dd=0:(datenum(num2str(ed),'yyyymmdd')-datenum(num2str(sd),'yyyymmdd'))
    jsrq=datestr(datestr(datenum(num2str(sd),'yyyymmdd')+dd),'yyyymmdd');    % 计算的日期
    for h1=1:height(DXUser)
        XD=outerjoin(DXUM(strcmp(DXUM.DATA_DATE,jsrq),[1,2,find(dx_userid==DXUser.METER_ID(h1))+2]),...
            TME(strcmp(TME.DATA_DATE,jsrq),:),'Keys',{'DATA_DATE','TIME'},'RightVariables',...
            TME.Properties.VariableNames(3:end),'Type','right');
        XD=fillmissing(XD,'movmedian',12,'DataVariables',XD.Properties.VariableNames(3:end),...
            'EndValues','nearest');
        XD=fillmissing(XD,'constant',0,'DataVariables',XD.Properties.VariableNames(3:end));
%         uxg=Tanimoto(XD{:,3},XD{:,4:end});
        uxg=corr((XD{:,3}-zsc{strcmp(zsc.DATA_DATE,jsrq),3:end}),XD{:,4:end},'rows',...
            'pairwise','type','Pearson');
        dxCU(h1,:)=transpose(diag(uxg));
%         dxCDU(h1,:)=duxg(1,2:end);
        clear XD uxg duxg;
    end
%     [mcdx,loc_mcdx]=max(dxCU,[],2);
%     DX_MID_RE=[DX_MID_RE;[DXUser,cell2table(cellstr(repmat(jsrq,height(DXUser),1)),'VariableNames',...
%         {'DATA_DATE'}),PBTG(loc_mcdx,[1,3,2]),array2table(mcdx,'VariableNames',{'DailyCorr'})]];
    mx_dd(:,:,dd+1)=double(dxCU==max(dxCU,[],2));             % 概率模型计算
%     mx_dd(:,:,dd+1)=dxCU;                                       % 累加模型计算
    
end
[zdx_d,mCU]=max(nanmean(mx_dd,3),[],2);
JG_DXU=[DXUser,PBTG(mCU,[1,3,2]),array2table(zdx_d,'VariableNames',{'MCorr'})];



% % ------------------------------------------------ 三相用户分析
sxCU=zeros(height(SXUser),length(SX)/3,6);                          % 三相用户与配变三相的6种组合相关性计算
sxCDU=zeros(height(SXUser),length(SX)/3,6);
SX_MID_RE=[];
for ds=0:(datenum(num2str(ed),'yyyymmdd')-datenum(num2str(sd),'yyyymmdd'))
    jsrq=datestr(datestr(datenum(num2str(sd),'yyyymmdd')+ds),'yyyymmdd');    % 计算的日期
    for h2=1:height(SXUser)
        sxum=SXUM(strcmp(SXUM.DATA_DATE,jsrq),[1:2,(find(sx_userid==SXUser.METER_ID(h2))+2)']);
        for h3=1:length(SX)/3
            smm=outerjoin(sxum,TME(strcmp(TME.DATA_DATE,jsrq),[1:2,3*h3:(3*h3+2)]),'Keys',...
                {'DATA_DATE','TIME'},'RightVariables',TME.Properties.VariableNames(3*h3:(3*h3+2)),...
                'Type','right');
            smm=fillmissing(smm,'movmedian',12,'DataVariables',smm.Properties.VariableNames(3:end),...
                'EndValues','nearest');
            smm=fillmissing(smm,'constant',0,'DataVariables',smm.Properties.VariableNames(3:end));


            mm=TPS(smm{:,3:5},smm{:,6:8},1)./3;
            %             mn=TPS(diff(smm{:,3:5},1,1),diff(smm{:,6:8},1,1));
            sxCU(h2,h3,:)=mm;
            %             sxCDU(h2,h3,:)=mn;
            clear smm mm;
        end
        clear sxum;
    end
%     ls_pbtg=PBTG(strcmp(PBTG.PHASE,'UA'),:);
%     [mcsx,loc_mcsx]=max(sxCU,[],3);
%     [mctg,loc_mctg]=max(mcsx,[],2);
%     dsxxx=diag(loc_mcsx(:,loc_mctg));
%     SX_MID_RE=[SX_MID_RE;[SXUser,cell2table(cellstr(repmat(jsrq,height(SXUser),1)),'VariableNames',...
%         {'DATA_DATE'}),ls_pbtg(loc_mctg,[1,3]),array2table([dsxxx,mctg],'VariableNames',...
%         {'PhaseSequence','DailyCorr'})]];
        mx_ss(:,:,:,ds+1)=double(sxCU==max(sxCU,[],3));         % 多日最大概率模型计算
%     mx_ss(:,:,:,ds+1)=sxCU;                                   % 多日最大累加模型计算
end

[zdxx,loc_zdxx]=max(nanmean(mx_ss,4),[],3);
[zdx_s,loc_zdtg]=max(zdxx,[],2);                 % 最大台区相似度数值，最大相似度对应台区的数值；
atg=PBTG(strcmp(PBTG.PHASE,'UA'),:);
sxxx=diag(loc_zdxx(:,loc_zdtg));
JG_SXU=[SXUser,atg(loc_zdtg,[1,3]),array2table([sxxx,zdx_s],'VariableNames',{'PhaseSequence','MCorr'})];





%% 


% % ----------- 准确率计算
% ZQL=array2table(100.*[height(JG_DXU(JG_DXU.TG_ID==JG_DXU.TGID,:))./ height(JG_DXU),...
%     height(JG_SXU(JG_SXU.TG_ID==JG_SXU.TGID,:)) ./ height(JG_SXU);...
%     height(JG_DXU(JG_DXU.TG_ID==JG_DXU.TGID,:))./ height(JG_DXU),...
%     height(JG_SXU(JG_SXU.TG_ID==JG_SXU.TGID & JG_SXU.PhaseSequence==1,:))./height(JG_SXU)],...
%     'VariableNames',{'DXU','SXU',},'RowNames',{'TG-User','TG-Phase-User'});

PhaseFlag=table([1;2;3],{'UA';'UB';'UC'},'VariableNames',{'PhaseNo','PhaseMark'});
PhaseSequence=table(transpose(1:6),[123;132;213;231;312;321],'VariableNames',{'PhaseNo','PhaseMark'});
JG_DXU=outerjoin(JG_DXU,PhaseFlag,'LeftKeys','PHASE','RightKeys','PhaseMark',...
    'RightVariables',{'PhaseNo'},'Type','left');
JG_DXU=outerjoin(JG_DXU,DXUIntegrality,'Keys','METER_ID','RightVariables',...
    'DXUserDataIntegrality','Type','left');
JG_DXU.Properties.VariableNames(10:11)={'JG_PhaseFlag','UserDataIntegrality'};

JG_SXU=outerjoin(JG_SXU,PhaseSequence,'LeftKeys','PhaseSequence','RightKeys','PhaseNo',...
    'RightVariables',{'PhaseMark'},'Type','left');
JG_SXU=outerjoin(JG_SXU,SXUIntegrality,'Keys','METER_ID','RightVariables',...
    'mean_SXUserDataIntegrality','Type','left');
JG_SXU.Properties.VariableNames(10:11)={'JG_PhaseFlag','UserDataIntegrality'};

% ------------------------------------------- 每天的输出结果规整
% DX_MID_RE=outerjoin(DX_MID_RE,PhaseFlag,'LeftKeys','PHASE','RightKeys','PhaseMark',...
%     'RightVariables',{'PhaseNo'},'Type','left');
% DX_MID_RE.Properties.VariableNames(end)={'JG_PhaseFlag'};
% SX_MID_RE=outerjoin(SX_MID_RE,PhaseSequence,'LeftKeys','PhaseSequence','RightKeys','PhaseNo',...
%     'RightVariables',{'PhaseMark'},'Type','left');
% SX_MID_RE.Properties.VariableNames(end)={'JG_PhaseFlag'};
% 
% DailyResult=[DX_MID_RE(:,{'ORG_NO','TG_ID','METER_ID','WIRING_MODE','PHASE_FLAG','DailyCorr',...
%     'TransMeterID','TGID','JG_PhaseFlag','DATA_DATE'});SX_MID_RE(:,{'ORG_NO','TG_ID','METER_ID',...
%     'WIRING_MODE','PHASE_FLAG','DailyCorr','TransMeterID','TGID','JG_PhaseFlag','DATA_DATE'})];
% DailyResult=outerjoin(DailyResult,TGTopo,'Keys',{'ORG_NO','TG_ID','METER_ID'},'RightVariables',...
%     {'ID'},'Type','left');
% DailyResult=DailyResult(:,[1:3,end,4:end-1]);

% ------------------------------------------ 最终辨识结果规整
JGResult=[JG_DXU(:,{'ORG_NO','TG_ID','METER_ID','WIRING_MODE','PHASE_FLAG','MCorr','TransMeterID','TGID',...
    'JG_PhaseFlag','UserDataIntegrality'});JG_SXU(:,{'ORG_NO','TG_ID','METER_ID','WIRING_MODE',...
    'PHASE_FLAG','MCorr','TransMeterID','TGID','JG_PhaseFlag','UserDataIntegrality'})];

JGResult=outerjoin(JGResult,TMIntegrality,'LeftKeys','TG_ID','RightKeys','TGID',...
    'RightVariables','mean_TransDataIntegrality','Type','left');
JGResult.Properties.VariableNames(11)={'TransDataIntegrality'};
JGResult=outerjoin(JGResult,TGTopo,'Keys',{'ORG_NO','TG_ID','METER_ID'},'RightVariables',...
    {'ID'},'Type','left');
JGResult=JGResult(:,[1:3,end,4:end-1]);

T=datestr(datetime('now','TimeZone','local','Format','yyyy-MM-dd'),'yyyy-mm-dd');
JGResult=[JGResult,cell2table(repmat(cellstr([string([sd,ed]),string(T)]),height(JGResult),1),'VariableNames',...
    {'StartDate','EndDate','CreateTime'})];
% DailyResult=[DailyResult,cell2table(repmat(cellstr(T),height(DailyResult),1),'VariableNames',{'CreateTime'})];




%%
% % ---------------------------- Internal Function Definition

    function fd = FEEL(AA)
        %  FEEL = Find Equal Element Location
        ue=unique(AA);
        fd=zeros(length(AA),1);
        for k=1:length(ue)
            
            aa=find(strcmp(AA,ue(k))==1);
            aa=sort(aa,'ascend');
            fd((3*k-2):(3*k),1)=aa;
            
        end
    end


    function sxxg=TPS(MX,MY,tp)
        XX=[1,2,3;1,3,2;2,1,3;2,3,1;3,1,2;3,2,1];
        if tp==1
        
        sxxg=nansum([diag(corr(MX(:,XX(1,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(2,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(3,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(4,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(5,:)),MY,'Type','Pearson','Rows','pairwise')),...
            diag(corr(MX(:,XX(6,:)),MY,'Type','Pearson','Rows','pairwise'))],1);
        else
            sxxg=[Tanimoto(reshape(MX(:,XX(1,:)),3*length(MX),1),reshape(MY,3*length(MY),1)),...
                Tanimoto(reshape(MX(:,XX(2,:)),3*length(MX),1),reshape(MY,3*length(MY),1)),...
                Tanimoto(reshape(MX(:,XX(3,:)),3*length(MX),1),reshape(MY,3*length(MY),1)),...
                Tanimoto(reshape(MX(:,XX(4,:)),3*length(MX),1),reshape(MY,3*length(MY),1)),...
                Tanimoto(reshape(MX(:,XX(5,:)),3*length(MX),1),reshape(MY,3*length(MY),1)),...
                Tanimoto(reshape(MX(:,XX(6,:)),3*length(MX),1),reshape(MY,3*length(MY),1))];
            
            
        end
        
    end

% % ---------------- Tanimoto 相似度计算
    function tnt=Tanimoto(tuv,ttv)
        tnt=nansum(tuv.*ttv,1)/(nansum(tuv.^2,1)+nansum(ttv.^2,1)-nansum(tuv.*ttv,1));
        
    end




% end


