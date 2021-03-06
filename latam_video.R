rm=(list=ls())
require(WRS)
require(RPresto)
require(psych)
require(mvoutlier)
require(survival)
require(pROC)
require(logistf)
require(bartMachine)
require(elasticnet)
require(caTools)
require(BayesianFirstAid)
ConnectNFLXPresto = function(username) {
  con <- dbConnect(RPresto::Presto(), host = "http://proxy.dataeng.netflix.net", port = 8080, user = username, schema = "default", catalog = "hive")
  return(con)
}
con = ConnectNFLXPresto("shuang")
video=dbGetQuery(con,'select * from shuang.rm_latam_agg_v tablesample bernoulli (80)')
colnames(video)
summary(video)
apply(video,2,max)
video$viewability_50<-NULL
video$viewability_100<-NULL
video$tot_video_interactions_met_event_sum<-NULL
video$tot_video_replays_met_event_sum<-NULL
video$tot_video_stops_met_event_sum<-NULL
video$video_view_time_met_event_time_sec_sum<-NULL
video<-video[,c(1,2,3,4,5,6,7,9,10,28:38)]
colnames(video)
apply(video,2,max)
video$first_date<-as.Date(as.character(video$first_date),"%Y%m%d")
video$lag_days<-as.numeric(as.Date(as.character(video$signup_date2),"%Y%m%d")-video$first_date)
video$adjusted_lag<-video$lag_days+1
head(video)
video$event_cnt<-apply(video[,10:20],1,function(x){sum(x!=0)})
video$interaction<-ifelse(video$event_cnt>0,1,0)
video$prefetch_overlag<-video$prefetch_met_event_count/video$adjusted_lag
video$video_skip_overlag<-video$video_skip_met_event_sum/video$adjusted_lag
video$video_view_overlag<-video$video_view_count/video$adjusted_lag
video$video_play_overlag<-video$tot_video_plays_met_event_sum/video$adjusted_lag
video$video_mutes_overlag<-video$tot_video_mutes_met_event_sum/video$adjusted_lag
video$video_pauses_overlag<-video$tot_video_pauses_met_event_sum/video$adjusted_lag
video$video_unmutes_overlag<-video$tot_video_unmutes_met_event_sum/video$adjusted_lag
video$video_midpoint_overlag<-video$tot_video_midpoint_met_event_sum/video$adjusted_lag
video$video_firstquartile_overlag<-video$tot_video_first_quartile_met_event_sum/video$adjusted_lag
video$video_thirdquartile_overlag<-video$tot_video_third_quartile_met_event_sum/video$adjusted_lag
video$video_complete_overlag<-video$tot_video_complete_plays_met_event_sum/video$adjusted_lag
video$event_cntoverlag<-video$event_cnt/video$adjusted_lag
video$video_fullscreen_overlag<-video$video_fullscreen_met_event_sum/video$adjusted_lag
video<-subset(video,interaction==1)
nrow(video)
# sample= sample.split(video,SplitRatio = .75)
# train = subset(video,sample == TRUE)
# val = subset(video, sample == FALSE)

osid=dbGetQuery(con,"select distinct os_id,os_desc from dse.ad_dfa_os_d")
video<-merge(x=video,y=osid,by='os_id',all.x=TRUE)
video$os_desc<-ifelse(video$os_desc=='NA',paste('unknown',video$os_id,sep='_'),video$os_desc)
video[mapply(is.infinite, video)] <- NA
video[is.na(video)]<-0
video$interaction<-NULL
sub<-subset(video,signup==1)
nullsamp<-subset(video,signup==0)[sample(1:nrow(subset(video,signup==0)),6*nrow(sub),replace=F),]
vboosted<-rbind(sub,nullsamp)
table(vboosted$signup)

# require(logistf)
# solutionv<-logistf(data=vboosted,signup ~ visit_flag + os_cnt + os_desc + prefetch_overlag + video_view_overlag + video_skip_overlag + video_play_overlag
#                   + video_mutes_overlag + video_pauses_overlag + video_unmutes_overlag + video_midpoint_overlag + video_thirdquartile_overlag
#                   + video_firstquartile_overlag + video_complete_overlag + event_cntoverlag)
# backward(solutionv)
# 
# require(glmnet)
# vboosted$event_cnt<-NULL
# colnames(vboosted)
# x<-vboosted[,c(3,4,21,24:35)]
# y<-vboosted[,8]
# fit<-glmnet(model.matrix(~.-1,x),y,family='binomial',alpha=0.5)
# cvfit<-cv.glmnet(model.matrix(~.-1,x),y,family='binomial', type.measure = "class")
# plot(cvfit)
# cvfit$lambda.1se
# cvfit$lambda.min
# coef(cvfit, s = "lambda.min")
# coef(cvfit,s='lambda.1se')
# library(survival)
# y<-Surv(vboosted$adjusted_lag,vboosted$signup)
# fit <- glmnet(model.matrix(~.-1,x), y, family="cox",alpha=0.5)
# plot(fit, label=T)
# cv.fit <- cv.glmnet(model.matrix(~.-1,x),y, family="cox", alpha=0.5)
# plot(cv.fit)
# coef(cv.fit,s='lambda.1se')
# 

load('~/shuang_source/Richmedia/latam_vboosted.Rda')
#BART#
options(java.parameters = "-Xmx64000m")
require(bartMachine)
load('~/shuang_source/Richmedia/latam_vboosted.Rda')
vboosted$video_fullscreen_overlag<-vboosted$video_fullscreen_met_event_sum/(vboosted$lag_days+1)

colnames(vboosted)
vboosted$event_cnt<-NULL
vboosted$os_desc<-NULL
vboosted$adjusted_lag<-NULL
colnames(vboosted)
vboosted$signup<-factor(vboosted$signup)
bmachine1<-build_bart_machine(vboosted[,c(3,21,22:34)],vboosted$signup)
summary(bmachine1)
var_selection_by_permute(bmachine1)
investigate_var_importance(bmachine1)


require(logistf)
stage1<-logistf(visit_flag ~ os_cnt + prefetch_overlag + video_view_overlag + video_skip_overlag + video_play_overlag
                + video_mutes_overlag + video_pauses_overlag + video_unmutes_overlag + video_midpoint_overlag + video_thirdquartile_overlag
                + video_firstquartile_overlag + video_complete_overlag + event_cntoverlag, data=vboosted)
stage1<-backward(stage1)
vboosted$visit_pred<-stage1$pred
vboosted2<-subset(vboosted,visit_flag==1)
test<-logistf(signup ~ os_cnt + prefetch_overlag + video_view_overlag + video_skip_overlag + video_play_overlag
              + video_mutes_overlag + video_pauses_overlag + video_unmutes_overlag + video_midpoint_overlag + video_thirdquartile_overlag
              + video_firstquartile_overlag + video_complete_overlag + event_cntoverlag, data=vboosted2)
stage2<-backward(test)

require(glmnet)
colnames(vboosted)
x<-vboosted[,c(4,21,24:35)]
y<-vboosted[,3]
fit<-glmnet(model.matrix(~.-1,x),y,family='binomial',alpha=0.5)
cvfit<-cv.glmnet(model.matrix(~.-1,x),y,family='binomial', type.measure = "class")
plot(cvfit)
cvfit$lambda.1se
coef(cvfit,s='lambda.1se')
x<-vboosted2[,c(4,21,24:35)]
y<-vboosted2[,8]
fit<-glmnet(model.matrix(~.-1,x),y,family='binomial',alpha=0.5)
cvfit<-cv.glmnet(model.matrix(~.-1,x),y,family='binomial', type.measure = "class")
plot(cvfit)
cvfit$lambda.1se
coef(cvfit,s='lambda.1se')

x<-vboosted[,c(3,4,21,24:35)]
y<-vboosted[,8]
fit<-glmnet(model.matrix(~.-1,x),y,family='binomial',alpha=0.5)
cvfit<-cv.glmnet(model.matrix(~.-1,x),y,family='binomial', type.measure = "class")
plot(cvfit)
coef(cvfit,s='lambda.1se')

# MNL Approach


vboosted$dv<-ifelse(vboosted$visit_flag==1 & vboosted$signup==1,2,
                    ifelse(vboosted$visit_flag==1 & vboosted$signup==0,1,
                           0))
table(vboosted$dv)
vboosted$dv<-factor(vboosted$dv)
vboosted$dv_desc<-ifelse(vboosted$dv==1,'visit',ifelse(vboosted$dv==2,'visitandsignup',
                                                       'none'))
table(vboosted$dv_desc)
vboosted$dv_desc<-factor(vboosted$dv_desc)
vboosted$dv2<-relevel(vboosted$dv_desc,ref='none')
test <- multinom(dv2 ~os_cnt + prefetch_overlag + video_view_overlag + video_skip_overlag + video_play_overlag
                 + video_mutes_overlag + video_pauses_overlag + video_unmutes_overlag + video_midpoint_overlag + video_thirdquartile_overlag
                 + video_firstquartile_overlag + video_complete_overlag + event_cntoverlag,data=vboosted)
z <- summary(test)$coefficients/summary(test)$standard.errors
p <- (1 - pnorm(abs(z), 0, 1)) * 2

