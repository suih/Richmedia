v_full<-build_bart_machine(vboosted[,c(2,3,20,22:32,40)],vboosted$signup)
vboosted$signup<-factor(vboosted$signup)
v_full<-build_bart_machine(vboosted[,c(2,3,20,22:32,40)],vboosted$signup)
summary(v_full)
var_selection_by_permute(v_full)
v_full
objects(v_full)
v_full$model_matrix_training_data
objects(v_full)
investigate_var_importance(v_full)
boosted$signup<-factor(boosted$signup)
bart_machine2<-build_bart_machine(boosted[,c(3:4,37:65)], boosted$signup)
summary(bart_machine2_
)
summary(bart_machine2)
bart_machine2_cv<-bartMachineCV(boosted[,c(3:4,37:65)], boosted$signup)
bart_machine2_cv
var_selection_by_permute_cv(bart_machine2_cv,margin=20)
var_selection_by_permute_cv(bart_machine2_cv,margin(20))
var_selection_by_permute_cv(bart_machine2_cv)
