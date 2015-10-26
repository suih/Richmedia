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

require(nnet)
#Modeling as a two stage choice problem -->visit-->signup
# uesed nnet because it doesn't require data transformation

boosted$dv<-ifelse(boosted$visit_flag==1 & boosted$signup==1,2,
                   ifelse(boosted$visit_flag==1 & boosted$signup==0,1,
                          0))
table(boosted$dv)
boosted$dv<-factor(boosted$dv)
boosted$dv_desc<-ifelse(boosted$dv==1,'visit',ifelse(boosted$dv==2,'visitandsignup',
                                                     'none'))
table(boosted$dv_desc)
boosted$dv_desc<-factor(boosted$dv_desc)
boosted$dv2<-relevel(boosted$dv_desc,ref='none')
test <- multinom(dv2 ~  os_cnt+average_display_time+average_interactivity_time+average_motif_expansions_time+average_video_view_time+lag_days+prefetch_overlag+display_overlag+html5_overlag+dynamic_ad_overlag+interactivity_overlag+motif_expansions_overlag+motif_manual_closes_overlag+video_view_overlag+video_play_overlag+video_stop_overlag+video_mutes_overlag+video_pauses_overlag+video_unmutes_overlag+video_replays_overlag+video_midpoint_overlag+interactivity_impression_overlag+video_firstquartile_overlag+video_thirdquartile_overlag+video_interaction_overlag+video_complete_overlag+event_cntoverlag,data=boosted)

z <- summary(test)$coefficients/summary(test)$standard.errors
p <- (1 - pnorm(abs(z), 0, 1)) * 2

colnames(boosted)
require(glmnet)
# Applying regularization to the multinomial logistic regression
x<-boosted[,c(36:39,41:61)]
y<-boosted$dv2
mnl<-glmnet(model.matrix(~.-1,x),y,family='multinomial',type.multinomial='grouped')
cvfit=cv.glmnet(model.matrix(~.-1,x), y, family="multinomial", type.multinomial = "grouped", parallel = TRUE)
plot(cvfit)
coef<-coef(cvfit,s='lambda.1se')


