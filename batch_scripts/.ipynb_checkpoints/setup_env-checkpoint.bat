call cd ..
call conda env create -f environment.yml
call conda activate Fuel
call ipython kernel install --user --name=Fuel
pause