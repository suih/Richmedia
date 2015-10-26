load('~/source/Richmedia/Richmedia/usboosted.Rda')
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

require(sem)
test<-tsls(signup ~ visit_flag + event_cntoverlag,instruments=~ event_cntoverlag,data=boosted,family=binomial('probit'))
