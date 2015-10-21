rm=(list=ls())
require(RPresto)
require(psych)
require(mvoutlier)
require(survival)
require(pROC)
require(logistf)
ConnectNFLXPresto = function(username) {
  con <- dbConnect(RPresto::Presto(), host = "http://presto.master.dataeng.netflix.net", port = 8080, user = username, schema = "default", catalog = "hive")
  return(con)
}
con = ConnectNFLXPresto("shuang")
richmedia=dbGetQuery(con,"select * from shuang.richmedia_clean_v3")
viewtime=dbGetQuery(con,"select * from shuang.richmedia_viewtime_v2")
richmedia<-merge(x=richmedia,y=viewtime,by='user_id')
head(richmedia)
load("richmediaraw.Rda")
#Diagnostics and Data Cleaning #
summary(richmedia)
richmedia$in_stream_companion_clicks_met_event_sum<-NULL
richmedia$vast_redirect_errors_met_event_sum<-NULL
richmedia$full_screen_average_view_time_met_event_time_sec_sum<-NULL
richmedia$full_screen_video_completes_met_event_sum<-NULL
richmedia$full_screen_video_plays_met_event_sum<-NULL
richmedia$full_screen_impression_met_event_sum<-NULL
sapply(richmedia[,9:length(colnames(richmedia))],quantile,0.9999)
sapply(richmedia[,9:length(colnames(richmedia))],sd)
interaction<-subset(richmedia,select=c(user_id,interactivity_time_met_event_count,
                                       motif_expansions_met_event_sum,
                                       video_complete_plays_met_event_sum,
                                       video_pauses_met_event_sum,
                                       video_replays_met_event_sum,
                                       video_fullscreen_met_event_sum,
                                       video_unmutes_met_event_sum,
                                       video_third_quartile_met_event_sum,
                                       video_skip_met_event_sum,
                                       video_view_time_met_event_time_sec_sum,
                                       interactivity_time_met_event_time_sec_sum,
                                       interactivity_impressions_met_event_count,
                                       motif_manual_closes_met_event_sum,
                                       motif_expansions_met_event_time_sec_sum,
                                       video_plays_met_event_sum,
                                       video_interactions_met_event_sum,
                                       video_mutes_met_event_sum,
                                       video_midpoint_met_event_sum,
                                       video_stops_met_event_sum,
                                       video_first_quartile_met_event_sum,
                                       video_view_count))
interaction$event_cnt<-apply(interaction[,2:length(colnames(interaction))],1,function(x){sum(x!=0)})
head(interaction)
richmedia<-merge(x=richmedia,y=subset(interaction,select=c(user_id,event_cnt)),by='user_id')
head(richmedia)
richmedia$first_date<-as.Date(as.character(richmedia$first_date),"%Y%m%d")
richmedia$subscribe<-ifelse(richmedia$signup_date<99999999,1,0)
richmedia$interaction<-ifelse(richmedia$event_cnt>0,1,0)
richmedia$signup2<-ifelse(richmedia$signup_date==99999999,'20150615',richmedia$signup_date)
richmedia$signup2<-as.Date(richmedia$signup2,"%Y%m%d")
richmedia$lag_days<-as.numeric(richmedia$signup2-richmedia$first_date)
richmedia$adjusted_lag<-richmedia$lag_days+1
#Initial Analysis to see if rich media engagement really matters for subscription#
table(richmedia$subscribe,richmedia$interaction)
colnames(richmedia)
nrow(richmedia)
#For subsequent sessions, load data#
#Within groups of users who interacted with richmedia,how is the frequency of interaction related to conversion#
nrow(clean)
table(clean$subscribe,clean$interaction)
modeldata<-subset(richmedia,interaction==1)
colnames(modeldata)
clean<-data.frame(modeldata[complete.cases(sapply(modeldata[,9:(length(colnames(modeldata))-6)],function(x)ifelse(x>as.numeric(quantile(x,0.9999)),NA,x))),])
modeldata<-clean
modeldata$event_cntoverlag<-modeldata$event_cnt/modeldata$adjusted_lag
modeldata$prefetch_overlag<-modeldata$prefetch_met_event_count/modeldata$adjusted_lag
modeldata$display_overlag<-modeldata$display_time_met_event_count/modeldata$adjusted_lag
modeldata$interactivity_overlag<-modeldata$interactivity_time_met_event_count/modeldata$adjusted_lag
modeldata$motif_expansions_overlag<-modeldata$motif_expansions_met_event_sum/modeldata$adjusted_lag
modeldata$motif_manual_closes_overlag<-modeldata$motif_manual_closes_met_event_sum/modeldata$adjusted_lag
modeldata$video_view_overlag<-modeldata$video_view_count/modeldata$adjusted_lag
modeldata$video_play_overlag<-modeldata$video_plays_met_event_sum/modeldata$adjusted_lag
modeldata$video_skip_overlag<-modeldata$video_skip_met_event_sum/modeldata$adjusted_lag
modeldata$video_stop_overlag<-modeldata$video_stops_met_event_sum/modeldata$adjusted_lag
modeldata$video_mutes_overlag<-modeldata$video_mutes_met_event_sum/modeldata$adjusted_lag
modeldata$video_pauses_overlag<-modeldata$video_pauses_met_event_sum/modeldata$adjusted_lag
modeldata$video_unmutes_overlag<-modeldata$video_unmutes_met_event_sum/modeldata$adjusted_lag
modeldata$video_replays_overlag<-modeldata$video_replays_met_event_sum/modeldata$adjusted_lag
modeldata$video_midpoint_overlag<-modeldata$video_midpoint_met_event_sum/modeldata$adjusted_lag
modeldata$video_fullscreen_overlag<-modeldata$video_fullscreen_met_event_sum/modeldata$adjusted_lag
modeldata$interactivity_impression_overlag<-modeldata$interactivity_impressions_met_event_count/modeldata$adjusted_lag
modeldata$video_firstquartile_overlag<-modeldata$video_first_quartile_met_event_sum/modeldata$adjusted_lag
modeldata$video_thirdquartile_overlag<-modeldata$video_third_quartile_met_event_sum/modeldata$adjusted_lag
modeldata$video_interaction_overlag<-modeldata$video_interactions_met_event_sum/modeldata$adjusted_lag
modeldata$video_complete_overlag<-modeldata$video_complete_plays_met_event_sum/modeldata$adjusted_lag
modeldata$average_display_time <-modeldata$display_time_met_event_time_sec_sum/modeldata$display_time_met_event_count
modeldata$average_interactivity_time<-modeldata$interactivity_time_met_event_time_sec_sum/modeldata$interactivity_time_met_event_count
modeldata$average_motif_expansions_time <-modeldata$motif_expansions_met_event_time_sec_sum/modeldata$motif_expansions_met_event_sum
modeldata$average_video_view_time<-modeldata$video_view_time_met_event_time_sec_sum/modeldata$video_view_count
modeldata$html5_overlag<-modeldata$rich_media_html5_impressions_met_event_sum/modeldata$adjusted_lag
modeldata$dynamic_ad_overlag<-modeldata$dynamic_ad_impression_met_event_sum/modeldata$adjusted_lag
modeldata[mapply(is.infinite, modeldata)] <- NA
modeldata[is.na(modeldata)]<-0
summary(modeldata)

######

fadata<-subset(modeldata,select=c(user_id,lag_days,prefetch_overlag,html5_overlag,dynamic_ad_overlag,
                                  display_overlag,subscribe,event_cntoverlag,
                                  interactivity_overlag,
                                  motif_expansions_overlag,
                                  motif_manual_closes_overlag,
                                  video_view_overlag,
                                  video_play_overlag,
                                  video_skip_overlag,
                                  video_stop_overlag,
                                  video_mutes_overlag,
                                  video_pauses_overlag,
                                  video_unmutes_overlag,
                                  video_replays_overlag,
                                  video_midpoint_overlag,
                                  video_fullscreen_overlag,
                                  interactivity_impression_overlag,
                                  video_firstquartile_overlag,
                                  video_thirdquartile_overlag,
                                  video_interaction_overlag,
                                  video_complete_overlag,
                                  average_display_time ,
                                  average_interactivity_time,
                                  average_motif_expansions_time, 
                                  average_video_view_time))
#Factor Analysis
require(psych)
fa.parallel(fadata[,8:length(colnames(fadata))])
a<-scale(fadata[,8:length(colnames(fadata))])
fa.parallel(a)
a<-data.frame(cbind(a,fadata[,1:6]))
class(fadata)
covmat<-cov(fadata[,8:length(colnames(fadata))])
covmat2<-cov(a)
fa<-fa(r=covmat2,covvar=TRUE,nfactors = 5,n.obs=nrow(fadata),rotate = "varimax")
output<- factanal(fadata[,5:length(colnames(fadata))],factors = 2,n.obs=nrow(fadata),rotation = "promax",scores="regression")

#PRINCOMP on regularized metrics

prcomp(a[,8:length(colnames(a))])->b
summary(b)
pca<-principal(fadata[,8:length(colnames(fadata))],nfactors = 6,rotate="varimax")
pcadata<-data.frame(cbind(pca$scores,fadata[,1:6]))
require(logistf)
solutionpca<-logistf(data=pcadata,subscribe ~ prefetch_overlag + dynamic_ad_overlag + html5_overlag+ PC1 + PC2 + PC3 + PC4 +PC5 + PC6)
#not good.
comb<-data.frame(solutionpca$predict,solutionpca$y)
solution<-logistf(data=modeldata,subscribe ~ event_cntoverlag + prefetch_overlag + dynamic_ad_overlag + html5_overlag + video_skip_overlag + video_stop_overlag + video_mutes_overlag +
                    video_pauses_overlag + video_unmutes_overlag + video_midpoint_overlag + video_fullscreen_overlag + interactivity_impression_overlag + 
                    video_firstquartile_overlag + video_thirdquartile_overlag +video_interaction_overlag + video_complete_overlag + 
                    average_interactivity_time + average_motif_expansions_time + average_video_view_time)

solution<-logistf(data=modeldata,subscribe ~ event_cntoverlag + prefetch_overlag + dynamic_ad_overlag + html5_overlag + video_skip_overlag + video_stop_overlag + video_mutes_overlag +
                    video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag + video_fullscreen_overlag + 
                    interactivity_impression_overlag + motif_expansions_overlag + motif_manual_closes_overlag + video_firstquartile_overlag + 
                    video_thirdquartile_overlag +video_interaction_overlag + video_complete_overlag + average_display_time +
                    average_interactivity_time + average_motif_expansions_time + average_video_view_time)

solution<-logistf(data=modeldata,subscribe ~ event_cntoverlag + prefetch_overlag + dynamic_ad_overlag + html5_overlag + video_skip_overlag + video_stop_overlag + video_mutes_overlag +
                    video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag + video_fullscreen_overlag + 
                    interactivity_impression_overlag + motif_expansions_overlag + motif_manual_closes_overlag + video_firstquartile_overlag + 
                    video_thirdquartile_overlag +video_interaction_overlag + video_complete_overlag + average_display_time +
                    average_interactivity_time + average_motif_expansions_time + average_video_view_time)

solution<-logistf(data=modeldata,subscribe ~ prefetch_overlag + event_cntoverlag + interactivity_overlag + motif_expansions_overlag + 
                    motif_manual_closes_overlag + video_view_overlag + video_play_overlag + 
                    video_skip_overlag + video_stop_overlag + video_mutes_overlag + 
                    video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + 
                    video_midpoint_overlag + 
                    video_fullscreen_overlag + 
                    interactivity_impression_overlag + 
                    video_firstquartile_overlag + 
                    video_thirdquartile_overlag + 
                    video_interaction_overlag + 
                    video_complete_overlag + 
                    average_display_time  + 
                    average_interactivity_time + 
                    average_motif_expansions_time +  
                    average_video_view_time)

solution<-logistf(data=modeldata,subscribe ~ event_cntoverlag + prefetch_overlag + dynamic_ad_overlag + html5_overlag + video_skip_overlag + video_stop_overlag +
                    video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag + video_fullscreen_overlag + 
                    motif_expansions_overlag + motif_manual_closes_overlag + video_firstquartile_overlag + video_interaction_overlag + 
                    average_display_time +average_interactivity_time + average_motif_expansions_time + average_video_view_time)





trimmodel<-subset(modeldata,lag_days<=28)

solution<-logistf(data=trimmodel,subscribe ~ event_cntoverlag + prefetch_overlag + dynamic_ad_overlag + html5_overlag + video_skip_overlag + video_stop_overlag +
                    video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag + video_fullscreen_overlag + 
                    motif_expansions_overlag + motif_manual_closes_overlag + video_firstquartile_overlag + video_interaction_overlag + 
                    average_display_time +average_interactivity_time + average_motif_expansions_time + average_video_view_time)

trimmodel2<-subset(trimmodel,select=c(user_id,lag_days,subscribe,event_cnt,dynamic_ad_impression_met_event_sum,rich_media_html5_impressions_met_event_sum,prefetch_met_event_count,
                                     interactivity_time_met_event_count,motif_expansions_met_event_sum,video_complete_plays_met_event_sum,
                                     video_pauses_met_event_sum,video_fullscreen_met_event_sum,
                                     video_unmutes_met_event_sum,video_third_quartile_met_event_sum,video_skip_met_event_sum,
                                     interactivity_impressions_met_event_count,motif_manual_closes_met_event_sum,
                                     video_plays_met_event_sum,video_interactions_met_event_sum,video_mutes_met_event_sum,
                                     video_midpoint_met_event_sum,video_stops_met_event_sum,video_first_quartile_met_event_sum,video_view_count,average_display_time,average_video_view_time,average_interactivity_time,average_motif_expansions_time))

cormat<-cor(trimmodel[,3:length(colnames(trimmodel))])

#Trimmed and regularized#
trim2<-subset(trimmodel,select=c(user_id,lag_days,subscribe,event_cntoverlag,prefetch_overlag,display_overlag,
                                 video_view_overlag,video_play_overlag,video_skip_overlag,video_stop_overlag,video_mutes_overlag, 
                                 video_pauses_overlag,video_unmutes_overlag,video_midpoint_overlag,video_fullscreen_overlag,
                                 interactivity_impression_overlag,video_firstquartile_overlag,video_thirdquartile_overlag,video_interaction_overlag,video_complete_overlag,average_display_time,
                                 average_interactivity_time,average_motif_expansions_time,average_video_view_time))
cormat2<-cor(trim2[,3:length(colnames(trim2))])

solution<-logistf(data=modeldata,subscribe ~ prefetch_overlag + html5_overlag + dynamic_ad_overlag + display_overlag + event_cntoverlag + interactivity_overlag + motif_expansions_overlag + 
                    motif_manual_closes_overlag + video_view_overlag + video_play_overlag + video_skip_overlag + video_stop_overlag + video_mutes_overlag + 
                    video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag + video_fullscreen_overlag + 
                    interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag + video_interaction_overlag + 
                    video_complete_overlag + average_display_time  + average_interactivity_time + average_motif_expansions_time +  average_video_view_time)
dropl(solution)
solution2<-logistf(data=trim2,subscribe ~ event_cntoverlag + prefetch_overlag + video_view_overlag + video_play_overlag + 
                    video_skip_overlag + video_stop_overlag + video_mutes_overlag +  video_pauses_overlag + video_unmutes_overlag  + 
                    video_midpoint_overlag + video_fullscreen_overlag + interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag +
                    video_interaction_overlag + video_complete_overlag + average_interactivity_time + average_motif_expansions_time + average_video_view_time)
solution2<-logistf(data=modeldata,subscribe ~ event_cntoverlag + prefetch_overlag + video_view_overlag + video_play_overlag + 
                     video_skip_overlag + video_stop_overlag + video_mutes_overlag +  video_pauses_overlag + video_unmutes_overlag  + 
                     video_midpoint_overlag + video_fullscreen_overlag + interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag +
                     video_interaction_overlag + video_complete_overlag + average_interactivity_time + average_motif_expansions_time + average_video_view_time)

fadata<-subset(modeldata,select=c(user_id,subscribe,signup2,first_date,event_cntoverlag,prefetch_overlag,display_overlag,
video_view_overlag,video_play_overlag,video_skip_overlag,video_stop_overlag,video_mutes_overlag,
video_pauses_overlag,video_unmutes_overlag,video_replays_overlag,video_midpoint_overlag,video_fullscreen_overlag,
interactivity_impression_overlag,video_firstquartile_overlag,video_thirdquartile_overlag,video_interaction_overlag,video_complete_overlag,average_display_time,
average_interactivity_time,average_motif_expansions_time,average_video_view_time))
modeldata[mapply(is.infinite, modeldata)] <- NA
modeldata[is.na(modeldata)]<-0

solution2<-logistf(formula=subscribe ~ event_cntoverlag + prefetch_overlag + display_overlag + video_view_overlag + video_play_overlag +
video_skip_overlag + video_stop_overlag + video_mutes_overlag +  video_pauses_overlag + video_unmutes_overlag  +
video_midpoint_overlag + video_fullscreen_overlag + interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag +
video_interaction_overlag + video_complete_overlag + average_interactivity_time + average_motif_expansions_time + average_video_view_time,data=fadata)

library(pROC)
comb<-data.frame(solution$y,solution$pred)
g <- roc(solution.y ~ solution.pred,data=comp)
plot(g)

#SURVIVAL ANALYSIS#
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

survival<-coxph(Surv(lag_days,subscribe) ~ 
                  event_cnt+interactivity_time_met_event_time_sec_sum+interactivity_time_met_event_count +interactivity_impressions_met_event_count+motif_manual_closes_met_event_sum + 
                  motif_expansions_met_event_time_sec_sum+motif_expansions_met_event_sum +video_plays_met_event_sum+video_view_count+video_view_time_met_event_time_sec_sum +
                  video_complete_plays_met_event_sum +video_interactions_met_event_sum +video_pauses_met_event_sum +video_mutes_met_event_sum+video_replays_met_event_sum + 
                  video_midpoint_met_event_sum +video_fullscreen_met_event_sum +video_stops_met_event_sum+video_unmutes_met_event_sum + video_first_quartile_met_event_sum +
                  video_third_quartile_met_event_sum +video_skip_met_event_sum,data=survdata2)

solution<-logistf(data=modeldata,subscribe ~ event_cntoverlag + prefetch_overlag + display_overlag + video_view_overlag + video_play_overlag + 
                    video_skip_overlag + video_stop_overlag + video_mutes_overlag +  video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + 
                    video_midpoint_overlag + video_fullscreen_overlag + interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag +
                    video_interaction_overlag + video_complete_overlag + average_display_time + average_interactivity_time + average_motif_expansions_time + average_video_view_time)




cor(richmedia$interaction,richmedia$subscribe)
cor(richmedia$event_cnt,richmedia$subscribe)
richmedia[is.infinite(richmedia)]<-NA
richmedia[is.na(richmedia)]<-0
summary(richmedia)


require(dplyr)
richmedia<-richmedia %>% 
  group_by(user_id) %>% 
  summarize(check=stderr(richmedia[,10:43])) %>% 
  ungroup

richmedia[is.na(richmedia)]<-0
fit <- princomp(subset(modeldata,select=-c(user_id,conversion_1day,conversion_3day,conversion_7day,conversion_14day,conversion_28day,signup_date,first_date,ident_user_cnt,subscribe,average_motif_expansions_time,average_video_view_time,full_screen_average_view_time_met_event_time_sec_sum,full_screen_video_plays_met_event_sum,full_screen_video_completes_met_event_sum,full_screen_impression_met_event_sum,vast_redirect_errors_met_event_sum,in_stream_companion_clicks_met_event_sum)), cor=TRUE)
summary(fit) 
loadings(fit)  
plot(fit,type="lines") 
fit$scores 
biplot(fit)

solution<-logistf(data=modeldata,subscribe ~ event_cntoverlag + prefetch_overlag + display_overlag + video_view_overlag + video_play_overlag + video_skip_overlag + video_stop_overlag + video_mutes_overlag +  video_pauses_overlag + video_unmutes_overlag + video_replays_overlag + video_midpoint_overlag + video_fullscreen_overlag + interactivity_impression_overlag + video_firstquartile_overlag + video_thirdquartile_overlag + video_interaction_overlag + video_complete_overlag)

fit <- princomp(subset(subset(richmedia,interaction==1),select=c(user_id,interactivity_time_met_event_time_sec_sum,interactivity_time_met_event_count ,
                                                                 interactivity_impressions_met_event_count,motif_manual_closes_met_event_sum,
                                                                 motif_expansions_met_event_time_sec_sum,motif_expansions_met_event_sum ,
                                                                 video_plays_met_event_sum,video_view_count,video_view_time_met_event_time_sec_sum ,
                                                                 video_complete_plays_met_event_sum ,video_interactions_met_event_sum ,video_pauses_met_event_sum ,
                                                                 video_mutes_met_event_sum,video_replays_met_event_sum,video_midpoint_met_event_sum ,video_fullscreen_met_event_sum ,
                                                                 video_stops_met_event_sum,video_unmutes_met_event_sum,video_first_quartile_met_event_sum ,video_third_quartile_met_event_sum ,video_skip_met_event_sum)), cor=TRUE)
summary(fit) # print variance accounted for
loadings(fit) # pc loadings
plot(fit,type="lines") # scree plot
fit$scores # the principal components
biplot(fit)
x<-fit$scores # the principal components

#########VISUALIZATION#########
ggplot(modeldata, aes(x=lag_days)) + geom_histogram(binwidth=.5, colour="black", fill="white") + facet_grid(subscribe ~.) + theme_bw()
ggplot(trimmodel, aes(x=lag_days)) + geom_histogram(binwidth=.5, colour="black", fill="green") + facet_grid(subscribe ~.) + theme_bw() + ggtitle('Trimmed Distribution of Time to Conversion/Censor by Subscription Groups')
ggplot(trimmodel, aes(x=event_cnt)) + geom_histogram(binwidth=.5, colour="black", fill="green") + facet_grid(subscribe ~.) + theme_bw()
