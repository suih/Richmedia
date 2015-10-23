# Rich Media Global Model --LATAM  OCT,2015
rm(list=ls())
ConnectNFLXPresto = function(username) {
  con <- dbConnect(
      RPresto::Presto(),
      host='http://proxy.dataeng.netflix.net',
      port=8080,
      user=username,
      schema='default',
      catalog='hive'
    )
  return (con)
}
con=ConnectNFLXPresto('shuang')
source('~/scicomp_utils/R/RPrestoClients/R_BigData.R')
require(WRS)
require(RPresto)
require(psych)
require(mvoutlier)
require(survival)
require(pROC)
richmedia<-data.frame(RPrint_CSV("shuang","select * from shuang.rm_latam_agg tablesample bernoulli (30)", file_out = '/volmount/latam',force_refresh=T))
richmedia$viewability_50<-NULL
richmedia$viewability_100<-NULL
richmedia$in_stream_companion_clicks_met_event_sum<-NULL
richmedia$vast_redirect_errors_met_event_sum<-NULL
richmedia$full_screen_average_view_time_met_event_time_sec_sum<-NULL
richmedia$full_screen_video_completes_met_event_sum<-NULL
richmedia$full_screen_video_plays_met_event_sum<-NULL
richmedia$full_screen_impression_met_event_sum<-NULL
richmedia$video_skip_met_event_sum<-NULL
richmedia$video_fullscreen_met_event_sum<-NULL
apply(richmedia,2,max)
colnames(richmedia)
richmedia$first_date<-as.Date(as.character(richmedia$first_date),"%Y%m%d")
richmedia$lag_days<-as.numeric(as.Date(as.character(richmedia$signup_date2),"%Y%m%d")-richmedia$first_date)
richmedia$adjusted_lag<-richmedia$lag_days+1
richmedia<-richmedia[,c(1:12,17,20:21,35:36,13:16,18:19,22:34)]
head(richmedia)
richmedia$prefetch_overlag<-richmedia$prefetch_met_event_count/richmedia$adjusted_lag
richmedia$display_overlag<-richmedia$display_time_met_event_count/richmedia$adjusted_lag
richmedia$html5_overlag<-richmedia$rich_media_html5_impressions_met_event_sum/richmedia$adjusted_lag
richmedia$dynamic_ad_overlag<-richmedia$dynamic_ad_impression_met_event_sum/richmedia$adjusted_lag
richmedia$interactivity_overlag<-richmedia$interactivity_time_met_event_count/richmedia$adjusted_lag
richmedia$motif_expansions_overlag<-richmedia$motif_expansions_met_event_sum/richmedia$adjusted_lag
richmedia$motif_manual_closes_overlag<-richmedia$motif_manual_closes_met_event_sum/richmedia$adjusted_lag
richmedia$video_view_overlag<-richmedia$video_view_count/richmedia$adjusted_lag
richmedia$video_play_overlag<-richmedia$tot_video_plays_met_event_sum/richmedia$adjusted_lag
richmedia$video_stop_overlag<-richmedia$tot_video_stops_met_event_sum/richmedia$adjusted_lag
richmedia$video_mutes_overlag<-richmedia$tot_video_mutes_met_event_sum/richmedia$adjusted_lag
richmedia$video_pauses_overlag<-richmedia$tot_video_pauses_met_event_sum/richmedia$adjusted_lag
richmedia$video_unmutes_overlag<-richmedia$tot_video_unmutes_met_event_sum/richmedia$adjusted_lag
richmedia$video_replays_overlag<-richmedia$tot_video_replays_met_event_sum/richmedia$adjusted_lag
richmedia$video_midpoint_overlag<-richmedia$tot_video_midpoint_met_event_sum/richmedia$adjusted_lag
richmedia$interactivity_impression_overlag<-richmedia$interactivity_impressions_met_event_count/richmedia$adjusted_lag
richmedia$video_firstquartile_overlag<-richmedia$tot_video_first_quartile_met_event_sum/richmedia$adjusted_lag
richmedia$video_thirdquartile_overlag<-richmedia$tot_video_third_quartile_met_event_sum/richmedia$adjusted_lag
richmedia$video_interaction_overlag<-richmedia$tot_video_interactions_met_event_sum/richmedia$adjusted_lag
richmedia$video_complete_overlag<-richmedia$tot_video_complete_plays_met_event_sum/richmedia$adjusted_lag
richmedia$average_display_time <-richmedia$display_time_met_event_time_sec_sum/richmedia$display_time_met_event_count
richmedia$average_interactivity_time<-richmedia$interactivity_time_met_event_time_sec_sum/richmedia$interactivity_time_met_event_count
richmedia$average_motif_expansions_time <-richmedia$motif_expansions_met_event_time_sec_sum/richmedia$motif_expansions_met_event_sum
richmedia$average_video_view_time<-richmedia$video_view_time_met_event_time_sec_sum/richmedia$video_view_count
richmedia$event_cnt<-apply(richmedia[,41:56],1,function(x){sum(x!=0)})
richmedia$interaction<-ifelse(richmedia$event_cnt>0,1,0)
#--Do we observe different engagement pattern among subscriber/nonsubscribers?
table(richmedia$interaction)
table(richmedia$interaction,richmedia$signup)
require(BayesianFirstAid)
# bayes.prop.test(c(25130,19620),c(9667226,5256018))
bayes.prop.test(c(2463,1927),c(967083,525199))
bayes.prop.test(c(5039,3829),c(1934365,1051673))
bayes.prop.test(c(7506,5905),c(2895984,1576496))
# SIGNIFICANT DIFFERENCES BETWEEN INTERACTION GROUPS FOUND IN ABOVE TEST: INTERACTION-->HIGHER CONVERSIONS
#--Get interaction data for model
modeldata<-subset(richmedia,interaction==1)
rm(list='richmedia')
modeldata[mapply(is.infinite, modeldata)] <- NA
modeldata[is.na(modeldata)]<-0
colnames(modeldata)
nrow(modeldata)
colnames(modeldata)
modeldata$interaction<-NULL
# Get device information
osid=RPrint_CSV("shuang","select distinct os_id,os_desc from dse.ad_dfa_os_d")
modeldata<-merge(x=modeldata,y=osid,by='os_id',all.x=TRUE)
modeldata$event_cntoverlag<-modeldata$event_cnt/modeldata$adjusted_lag
modeldata$os_desc<-ifelse(modeldata$os_desc=='NA',paste('unknown',modeldata$os_id,sep='_'),modeldata$os_desc)
modeldata[mapply(is.infinite, modeldata)] <- NA
modeldata[is.na(modeldata)]<-0

modeldata$video_play_quadratic<-modeldata$video_play_overlag^2
modeldata$video_complete_quadratic<-modeldata$video_complete_overlag^2
modeldata$video_mutes_quadratic<-modeldata$video_mutes_overlag^2
modeldata$video_stop_quadratic<-modeldata$video_stop_overlag^2
modeldata$video_pause_quadratic<-modeldata$video_pauses_overlag^2
modeldata$event_cnt_quadratic<-modeldata$event_cntoverlag^2
modeldata$video_unmutes_quadratic<-modeldata$video_unmutes_overlag^2
modeldata$video_replays_quadratic<-modeldata$video_replays_overlag^2
modeldata$video_view_quadratic<-modeldata$video_view_overlag^2
modeldata$video_interaction_quadratic<-modeldata$video_interaction_overlag^2

# Down sampling the non converted customers for modeling purpose
sub<-subset(modeldata,signup==1)
nullsamp<-subset(modeldata,signup==0)[sample(1:nrow(subset(modeldata,signup==0)),6*nrow(sub),replace=F),]
boosted<-rbind(sub,nullsamp)
table(boosted$signup)


require(glmnet)
boosted$event_cnt<-NULL
colnames(boosted)
x<-boosted[,c(3,4,16,37:72)]
y<-boosted[,9]
fit<-glmnet(model.matrix(~.-1,x),y,family='binomial',alpha=0.5)
cvfit<-cv.glmnet(model.matrix(~.-1,x),y,family='binomial', type.measure = "class")
plot(cvfit)
cvfit$lambda.1se
cvfit$lambda.min
coef(cvfit, s = "lambda.min")
coef(cvfit,s='lambda.1se')
library(survival)
y<-Surv(boosted$adjusted_lag,boosted$signup)
fit <- glmnet(model.matrix(~.-1,x), y, family="cox",alpha=0.5)
plot(fit, label=T)
cv.fit <- cv.glmnet(model.matrix(~.-1,x),y, family="cox", alpha=0.5)
plot(cv.fit)
coef(cv.fit,s='lambda.1se')


#BART#
load('~/shuang_source/Richmedia/boosted.Rda')
options(java.parameters = "-Xmx64000m")
colnames(boosted)
boosted$event_cnt<-NULL
boosted$os_desc<-NULL
colnames(boosted)
bmachinecv<-bartMachineCV(boosted[,c(3,4,16,37:62)],boosted$signup)
var_selection_by_permute_cv(bmachinecv)
investigate_var_importance_cv(bmachinecv)
require(bartMachine)
bmachinecv<-bartMachineCV(boosted[,c(3,4,16,37:71)],boosted$signup)
var_selection_by_permute_cv(bmachinecv,margin=20)
investigate_var_importance(bmachinecv)

library(pROC)
comb<-data.frame(solution2$y,solution2$pred)
g <- roc(solution2.y ~ solution2.pred,data=comb)
plot(g)

library(pROC)
comb<-data.frame(solutionfinal$y,solutionfinal$pred)
g <- roc(solutionfinal.y ~ solutionfinal.pred,data=comb)
plot(g)

fadata<-subset(modeldata,select=c(user_id,os_desc,lag_days,prefetch_overlag,html5_overlag,dynamic_ad_overlag,
 display_overlag,signup,event_cntoverlag,
 interactivity_overlag,
 motif_expansions_overlag,
 motif_manual_closes_overlag,
 video_view_overlag,
 video_play_overlag,
 video_stop_overlag,
 video_mutes_overlag,
 video_pauses_overlag,
 video_unmutes_overlag,
 video_replays_overlag,
 video_midpoint_overlag,
 interactivity_impression_overlag,
 video_firstquartile_overlag,
 video_thirdquartile_overlag,
 video_interaction_overlag,
 video_complete_overlag,
 average_display_time,
 average_interactivity_time,
 average_motif_expansions_time, 
 average_video_view_time))

summary(modeldata)

#Develop models using the training sample data set#

# logtrans<-log(fadata[,9:length(colnames(fadata))]+1)
# logtrans<-data.frame(cbind(fadata[,1:8],logtrans))

a<-scale(fadata[,10:length(colnames(fadata))])
fa.parallel(a)
a<-data.frame(cbind(fadata[,1:9],a))
covmat<-cov(a[,10:length(colnames(a))])
fa<-fa(r=covmat,covvar=TRUE,nfactors = 5,n.obs=nrow(a),rotate = "varimax")
prcomp(a[,10:length(colnames(a))])->b
summary(b)
pca<-principal(a[,10:length(colnames(a))],nfactors = 5, rotate="varimax")
pcadata<-data.frame(cbind(a[,1:9],pca$scores))
head(pcadata)

# Visualization of latent structure in rich media interactions
require(ggplot2)
theta <- seq(0,2*pi,length.out = 100)
circle <- data.frame(x = cos(theta), y = sin(theta))
p <- ggplot(circle,aes(x,y)) + geom_path()

loadings <- data.frame(unclass(pca$loadings), .names = row.names(unclass(pca$loadings)))
p + geom_text(data=loadings, 
  mapping=aes(x = PC1, y = PC2, label = .names, colour = .names)) +
  coord_fixed(ratio=1) +
  labs(x = "PC1", y = "PC2")

require(ggplot2)
theta <- seq(0,2*pi,length.out = 100)
circle <- data.frame(x = cos(theta), y = sin(theta))
p <- ggplot(circle,aes(x,y)) + geom_path()
loadings <- data.frame(b$rotation, .names = row.names(b$rotation)))
p + geom_text(data=loadings, 
  mapping=aes(x = PC1, y = PC2, label = .names, colour = .names)) +
  coord_fixed(ratio=1) +
  labs(x = "PC1", y = "PC2")

require(logistf)
solutionpca<-logistf(data=pcadata,signup ~ 
  event_cntoverlag + prefetch_overlag + dynamic_ad_overlag + html5_overlag+ 
  PC1 + PC2 + PC3 + PC4 + PC5 + PC6 + PC7 + os_desc )

solution<-logistf(data=modeldata,signup ~event_cntoverlag + os_cnt + prefetchtag + dynamictag + html5tag + interactivity_overlag + motif_expansions_overlag + 
  motif_manual_closes_overlag + video_view_overlag + video_play_overlag + video_stop_overlag + video_mutes_overlag + 
  video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag
  interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag + video_interaction_overlag + 
  video_complete_overlag + average_display_timeaverage_interactivity_time + average_motif_expansions_time +  average_video_view_time + os_desc)



install.packages('logistf')
install.packages('bartMachine')
require(logistf)
solution_boosted<-logistf(data=boosted,signup ~event_cntoverlag + prefetch_overlag + dynamic_ad_overlag + html5_overlag + interactivity_overlag + motif_expansions_overlag + 
 motif_manual_closes_overlag + video_view_overlag + video_play_overlag + video_stop_overlag + video_mutes_overlag + 
 video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag + 
 interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag + video_interaction_overlag + 
 video_complete_overlag + average_display_time + average_interactivity_time + average_motif_expansions_time +  average_video_view_time + os_desc)
backward(solution_boosted)



logistf(formula = subscribe ~ event_cntoverlag + prefetchtag + 
 dynamictag + motif_expansions_overlag + motif_manual_closes_overlag + 
 video_play_overlag + video_skip_overlag + video_stop_overlag + 
 video_mutes_overlag + video_pauses_overlag + video_unmutes_overlag + 
 video_replays_overlag + video_fullscreen_overlag + interactivity_impression_overlag + 
 video_thirdquartile_overlag + video_interaction_overlag + 
 average_display_time + average_interactivity_time + average_motif_expansions_time + 
 average_video_view_time, data = modeldata)

logistf(formula = subscribe ~ event_cntoverlag + prefetchtag + 
 dynamictag + 
 video_play_overlag + video_stop_overlag + 
 video_mutes_overlag + video_pauses_overlag +
 video_fullscreen_overlag + interactivity_impression_overlag + 
 video_thirdquartile_overlag + video_interaction_overlag + 
 average_display_time + average_interactivity_time + average_motif_expansions_time + 
 average_video_view_time, data = modeldata)

logistf(formula = subscribe ~ event_cntoverlag + prefetchtag + 
 dynamictag + video_view_overlag + video_skip_overlag + video_stop_overlag + 
 video_mutes_overlag + video_pauses_overlag + video_unmutes_overlag + 
 video_midpoint_overlag + video_fullscreen_overlag + interactivity_impression_overlag + 
 video_firstquartile_overlag + video_interaction_overlag + 
 average_display_time + average_interactivity_time + average_motif_expansions_time + 
 average_video_view_time + device, data = boosted)

logistf(formula = subscribe ~ event_cntoverlag + prefetchtag + 
 dynamictag + motif_expansions_overlag + motif_manual_closes_overlag + 
 video_play_overlag + video_skip_overlag + video_stop_overlag + 
 video_mutes_overlag + video_pauses_overlag + video_unmutes_overlag + 
 video_replays_overlag + video_fullscreen_overlag + interactivity_impression_overlag + 
 video_thirdquartile_overlag + video_interaction_overlag + 
 average_display_time + average_interactivity_time + average_motif_expansions_time + 
 average_video_view_time, data = modeldata)


library(survival)
survdata<-subset(modeldata,select=c(user_id,subscribe,adjusted_lag,lag_days,event_cnt,interactivity_time_met_event_time_sec_sum,interactivity_time_met_event_count ,
interactivity_impressions_met_event_count,motif_manual_closes_met_event_sum,
motif_expansions_met_event_time_sec_sum,motif_expansions_met_event_sum ,
video_plays_met_event_sum,video_view_count,video_view_time_met_event_time_sec_sum ,
video_complete_plays_met_event_sum ,video_interactions_met_event_sum ,video_pauses_met_event_sum ,
video_mutes_met_event_sum,video_replays_met_event_sum,video_midpoint_met_event_sum ,video_fullscreen_met_event_sum ,
video_stops_met_event_sum,video_unmutes_met_event_sum,video_first_quartile_met_event_sum ,video_third_quartile_met_event_sum,video_skip_met_event_sum))
survival<-coxph(Surv(lag_days,subscribe) ~ 
event_cnt+interactivity_time_met_event_time_sec_sum+interactivity_time_met_event_count +interactivity_impressions_met_event_count+motif_manual_closes_met_event_sum + 
motif_expansions_met_event_time_sec_sum+motif_expansions_met_event_sum +video_plays_met_event_sum+video_view_count+video_view_time_met_event_time_sec_sum +
video_complete_plays_met_event_sum +video_interactions_met_event_sum +video_pauses_met_event_sum +video_mutes_met_event_sum+video_replays_met_event_sum + 
video_midpoint_met_event_sum +video_fullscreen_met_event_sum +video_stops_met_event_sum+video_unmutes_met_event_sum + video_first_quartile_met_event_sum +
video_third_quartile_met_event_sum +video_skip_met_event_sum,data=survdata)


# Out of sample validataion
load('richmedia_validation2.Rda')
solution3<-logistf(data=modeldata,subscribe ~ event_cntoverlag + prefetchtag + dynamictag
motif_manual_closes_overlag + video_view_overlag + video_play_overlag + video_stop_overlag + video_mutes_overlag + 
video_pauses_overlag + video_fullscreen_overlag + 
interactivity_impression_overlag + video_interaction_overlag + 
video_complete_overlag + average_display_timeaverage_interactivity_time + average_motif_expansions_time +  average_video_view_time + device)

head(val)
valdata<-model.matrix(subscribe~ event_cntoverlag + prefetchtag + 
dynamictag + motif_manual_closes_overlag + video_view_overlag + 
video_play_overlag + video_stop_overlag + video_mutes_overlag + 
video_pauses_overlag + video_fullscreen_overlag + interactivity_impression_overlag + 
video_interaction_overlag + video_complete_overlag + average_display_time + 
average_interactivity_time + average_motif_expansions_time + 
average_video_view_time + device,data=modeldata2)
beta<-coef(solution3)

hier.part(boosted$subscribe,iv,family='binomial',gof="RMSPE",barplot=TRUE)

pi.obs <- 1 / (1 + exp(-valdata %*% beta))


# VISUALIZATION

require(ggplot2)
ggplot(modeldata, aes(x=lag_days)) + geom_histogram(binwidth=.5, colour="black", fill="white") + facet_grid(subscribe ~.,scales='free') + theme_bw()ggtitle('Distribution of Time to Conversion/Censor by Subscription Groups')

library(scatterplot3d)
loading<-as.data.frame(b$rotation)
loading<-unclass(fa$loadings)
with(loading,{
  s3d <- scatterplot3d(PC1,PC2,PC3, # x y and z axis
  color="blue", pch=19,  # filled blue circles
  type="h",  # vertical lines to the x-y plane
  main="Latent Structures",
  xlab="Dimension1",
  ylab="Dimension2",
  zlab="Dimension3")
  s3d.coords <- s3d$xyz.convert(PC1,PC2,PC3) # convert 3D coords to 2D projection
  text(s3d.coords$x, s3d.coords$y, # x and y coordinates
 labels=row.names(loading),# text to plot
 cex=.5, pos=4)  # shrink text 50% and place to right of points)
  
})
loading<-as.data.frame(unclass(fa$loadings))
with(loading,{
  s3d <- scatterplot3d(MR2,MR1,MR3, # x y and z axis
  color="blue", pch=19,  # filled blue circles
  type="h",  # vertical lines to the x-y plane
  main="Latent Structure of Rich Media Metrics",
  xlab="Dimension1",
  ylab="Dimension2",
  zlab="Dimension3")
  s3d.coords <- s3d$xyz.convert(MR2,MR1,MR3) # convert 3D coords to 2D projection
  text(s3d.coords$x, s3d.coords$y, # x and y coordinates
 labels=row.names(loading),# text to plot
 cex=.6, pos=4)  # shrink text 50% and place to right of points)
  
})

# GLMNET

require(glmnet)
colnames(boosted)
boosted$os_desc<-NULL
boosted$prefetchtag<-NULL
boosted$displaytag<-NULL
boosted$html5tag<-NULL
x<-boosted[,38:72]
summary(boosted$device)
y<-boosted$subscribe
fit<-glmnet(model.matrix(~.,x),y,family='binomial',alpha=0.5)
cvfit<-cv.glmnet(model.matrix(~.,x),y,family='binomial', type.measure = "class")
cvfit$lambda.1se
cvfit$lambda.min
coef(cvfit, s = "lambda.min")
x<-model.matrix(~event_cntoverlag + prefetchtag + dynamictag + interactivity_overlag + motif_expansions_overlag + 
motif_manual_closes_overlag + video_view_overlag + video_play_overlag + video_skip_overlag + video_stop_overlag + video_mutes_overlag + 
video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag + video_fullscreen_overlag + 
interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag + video_interaction_overlag + 
video_complete_overlag + average_display_timeaverage_interactivity_time + average_motif_expansions_time +  average_video_view_time-1,boosted)
colnames(boosted)

# SURVIVAL ANALYSIS USING AGGREGATED DATA
library(survival)
y<-Surv(boosted$adjusted_lag,boosted$subscribe)
fit <- glmnet(model.matrix(~.-1,x), y, family="cox",alpha=0.5)
plot(fit, label=T)
cv.fit <- cv.glmnet(model.matrix(~.-1,x),y, family="cox", alpha=0.5)
plot(cv.fit)

#SURVIVAL ANALYSIS USING EVENT LEVEL DATA



# Get Stratified Sample
sub<-subset(modeldata,subscribe==1)
nullsamp<-subset(modeldata,subscribe==0)[sample(1:nrow(subset(modeldata,subscribe==0)),6*nrow(sub),replace=F),]
boosted<-rbind(sub,nullsamp)
table(boosted$subscribe)
boosted$device<-ifelse(boosted$os_desc=='0',paste('unknown',boosted$os_id,sep='_'),boosted$os_desc)

subsamp<-subset(a[sample(1:nrow(a),.5*nrow(a),replace=F),])

