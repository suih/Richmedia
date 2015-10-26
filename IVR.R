rm(list=ls())
load('~/source/Richmedia/usboosted.Rda')
require(glmnet)
require(mediation)
colnames(boosted)
boosted$os_desc<-NULL
boosted$prefetchtag<-NULL
boosted$displaytag<-NULL
boosted$html5tag<-NULL
boosted$adjusted_lag<-NULL
colnames(boosted)

x<-boosted[,c(36:39,44:60)]
y<-boosted$visit_flag
fit<-glmnet(model.matrix(~.-1,x),y,family='binomial',alpha=0.5)
cvfit<-cv.glmnet(model.matrix(~.-1,x),y,family='binomial', type.measure = "class")
plot(cvfit)
coef(cvfit, s = "lambda.1se")

require(logistf)
stage1<-logistf(visit_flag ~ video_stop_overlag+interactivity_impression_overlag+video_interaction_overlag+event_cntoverlag+video_firstquartile_overlag+average_interactivity_time+average_motif_expansions_time+lag_days+video_view_overlag+video_midpoint_overlag+video_unmutes_overlag,data=boosted)
stage1<-backward(stage1)
boosted$visit_pred<-stage1$pred
boosted2<-subset(boosted,visit_flag==1)
test<-logistf(signup ~ os_cnt+average_display_time+average_interactivity_time+average_motif_expansions_time+average_video_view_time+lag_days+prefetch_overlag+display_overlag+html5_overlag+dynamic_ad_overlag+interactivity_overlag+motif_expansions_overlag+motif_manual_closes_overlag+video_view_overlag+video_play_overlag+video_stop_overlag+video_mutes_overlag+video_pauses_overlag+video_unmutes_overlag+video_replays_overlag+video_midpoint_overlag+interactivity_impression_overlag+video_firstquartile_overlag+video_thirdquartile_overlag+video_interaction_overlag+video_complete_overlag+event_cntoverlag,data=boosted2)
stage2<-backward(test)
