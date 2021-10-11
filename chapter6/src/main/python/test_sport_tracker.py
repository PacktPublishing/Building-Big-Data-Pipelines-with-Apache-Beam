import unittest

from sport_tracker import SportTrackerCalc
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class TestSportTracker(unittest.TestCase):

  data = [
     ('track1', (46.93945112545023, -27.960070478609623, 1603296466118.0)),
     ('track1', (46.93939633525373, -27.96014179360017, 1603296466122.0)),
     ('track1', (46.93958252662445, -27.960110171727298, 1603296466131.0)),
     ('track1', (46.93950173048603, -27.96020673289596, 1603296466137.0)),
     ('track1', (46.93944543756522, -27.960136598021787, 1603296466141.0)),
     ('track1', (46.93946015638307, -27.95992126328931, 1603296466151.0)),
     ('track1', (46.939263361513206, -27.959794988734018, 1603296466162.0)),
     ('track1', (46.93913244807838, -27.959623386393726, 1603296466172.0)),
     ('track1', (46.93921295418787, -27.959663468307895, 1603296466176.0)),
     ('track1', (46.939386259541465, -27.959738517783503, 1603296466185.0)),
     ('track1', (46.93948672959186, -27.95957860237353, 1603296466194.0)),
     ('track1', (46.939411010162615, -27.959780721790324, 1603296466204.0)),
     ('track1', (46.93946501128842, -27.9598944581244, 1603296466210.0)),
     ('track1', (46.93964136900637, -27.96008659427073, 1603296466222.0)),
     ('track1', (46.939759640561896, -27.960183473382994, 1603296466229.0)),
     ('track1', (46.93967036158386, -27.960194292698553, 1603296466233.0)),
     ('track1', (46.939576524999545, -27.960140990558852, 1603296466238.0)),
     ('track1', (46.93951750455507, -27.960252205047166, 1603296466244.0)),
     ('track1', (46.93947431530802, -27.960351104549652, 1603296466249.0)),
     ('track1', (46.93950347358672, -27.9604361785662, 1603296466253.0)),
     ('track1', (46.93942753778871, -27.960536606798577, 1603296466259.0)),
     ('track1', (46.93946995418515, -27.960655151830487, 1603296466265.0)),
     ('track1', (46.93934300477388, -27.960769523074884, 1603296466273.0)),
     ('track1', (46.93937764384891, -27.960955176795167, 1603296466282.0)),
     ('track1', (46.93952343955541, -27.961114328617573, 1603296466292.0)),
     ('track1', (46.93938811450963, -27.96118546830569, 1603296466299.0)),
     ('track1', (46.939291371912546, -27.960992526447818, 1603296466309.0)),
     ('track1', (46.939252211464364, -27.961140310691018, 1603296466316.0)),
     ('track1', (46.939084485489396, -27.96117294398518, 1603296466324.0)),
     ('track1', (46.93896404322046, -27.961404270601165, 1603296466336.0)),
     ('track1', (46.93919157227727, -27.961531741663435, 1603296466348.0)),
     ('track1', (46.93900799990443, -27.961487375535523, 1603296466357.0)),
     ('track1', (46.9389002480022, -27.961422247946576, 1603296466363.0)),
     ('track1', (46.93897506920023, -27.96147214365615, 1603296466367.0)),
     ('track1', (46.93880454484273, -27.96146126348769, 1603296466375.0)),
     ('track2', (54.80485140081757, 89.56415275626105, 1610554066149.0)),
     ('track2', (54.805172161504764, 89.56419669241271, 1610554066157.0)),
     ('track2', (54.80498415043149, 89.56413507458326, 1610554066162.0)),
     ('track2', (54.80487446376015, 89.56401602297639, 1610554066166.0)),
     ('track2', (54.804619521599534, 89.56390320369452, 1610554066173.0)),
     ('track2', (54.80438046364492, 89.5639457631707, 1610554066179.0)),
     ('track2', (54.80418951288444, 89.56420721223064, 1610554066187.0)),
     ('track2', (54.80442391927559, 89.56398389306966, 1610554066195.0)),
     ('track2', (54.80442267689047, 89.56370510614009, 1610554066202.0)),
     ('track2', (54.80421620677026, 89.56326554937678, 1610554066214.0)),
     ('track2', (54.80456964254658, 89.56300235539098, 1610554066225.0)),
     ('track2', (54.80474300566356, 89.56340748888974, 1610554066236.0)),
     ('track2', (54.8050466740701, 89.56372682166447, 1610554066247.0)),
     ('track2', (54.804802357491006, 89.56346278737632, 1610554066256.0)),
     ('track2', (54.80514999721523, 89.56355526071454, 1610554066265.0)),
     ('track2', (54.805385686732926, 89.5638270237499, 1610554066274.0)),
     ('track2', (54.80540157745064, 89.56366592770382, 1610554066278.0)),
     ('track2', (54.80558567709342, 89.56397497789015, 1610554066287.0)),
     ('track2', (54.80559058916283, 89.56369623146921, 1610554066294.0)),
     ('track2', (54.80532633513106, 89.56404887538145, 1610554066305.0)),
     ('track2', (54.80531756801325, 89.56424653179603, 1610554066310.0)),
     ('track2', (54.80550774174869, 89.56430111082983, 1610554066315.0)),
     ('track2', (54.805385503039595, 89.56400131839852, 1610554066323.0)),
     ('track2', (54.80519469795315, 89.56405364754498, 1610554066328.0)),
     ('track2', (54.804803185642385, 89.56415609718663, 1610554066338.0)),
     ('track2', (54.80440231677973, 89.56397308877945, 1610554066349.0)),
     ('track2', (54.804297993847776, 89.56380497677378, 1610554066354.0)),
     ('track2', (54.80424014523661, 89.56365378818573, 1610554066358.0)),
     ('track2', (54.80433548797761, 89.56391576806541, 1610554066365.0)),
     ('track2', (54.8046087940558, 89.56397079063571, 1610554066372.0)),
     ('track2', (54.80419055208527, 89.56421759767126, 1610554066384.0)),
     ('track2', (54.80404920141358, 89.56392632839467, 1610554066392.0)),
     ('track2', (54.80423604321113, 89.56408140952624, 1610554066398.0)),
     ('track2', (54.804596191251434, 89.56433534123202, 1610554066409.0)),
     ('track2', (54.80475895660141, 89.56415515395298, 1610554066415.0)),
     ('track2', (54.804489898830724, 89.56385285321765, 1610554066425.0)),
     ('track2', (54.80465035305271, 89.56413405109554, 1610554066433.0)),
     ('track2', (54.804498406382876, 89.5642607678273, 1610554066438.0)),
     ('track2', (54.8046905163782, 89.56421345323886, 1610554066443.0)),
     ('track2', (54.80472369889555, 89.56449026114366, 1610554066450.0)),
     ('track2', (54.80495498292735, 89.56471681246347, 1610554066458.0)),
     ('track2', (54.805205805981494, 89.56492152103532, 1610554066466.0)),
     ('track2', (54.80500877244116, 89.56490355705559, 1610554066471.0)),
     ('track2', (54.80482567856277, 89.56506304575855, 1610554066477.0)),
     ('track2', (54.804421880682085, 89.5650899732558, 1610554066487.0)),
     ('track2', (54.80415320400797, 89.56490933370127, 1610554066495.0)),
     ('track2', (54.804033553155655, 89.5646084990666, 1610554066503.0)),
     ('track2', (54.80434116925136, 89.56423271668758, 1610554066515.0)),
     ('track2', (54.80454630643912, 89.5639822440582, 1610554066523.0)),
     ('track2', (54.8042794557165, 89.5643879912092, 1610554066535.0)),
     ('track2', (54.804603187694255, 89.56439191682358, 1610554066543.0)),
     ('track2', (54.80442447124317, 89.56407972277134, 1610554066552.0)),
     ('track2', (54.804825538563755, 89.56402565963313, 1610554066562.0)),
     ('track2', (54.80508114171268, 89.56377253569826, 1610554066571.0)),
     ('track2', (54.80483788793508, 89.5634491080893, 1610554066581.0)),
     ('track2', (54.805039640139334, 89.5631959009568, 1610554066589.0)),
     ('track2', (54.805152132975046, 89.56331230462033, 1610554066593.0)),
     ('track2', (54.805590222321506, 89.56326470581085, 1610554066604.0)),
     ('track2', (54.80543128952752, 89.5631468699607, 1610554066609.0)),
     ('track2', (54.80587303314501, 89.56334861890151, 1610554066621.0)),
     ('track2', (54.80603292489055, 89.56316587688558, 1610554066627.0)),
     ('track2', (54.80627001974715, 89.56294541411583, 1610554066635.0)),
     ('track2', (54.80633822526471, 89.56267509633433, 1610554066642.0)),
     ('track2', (54.80611252643459, 89.56290721234403, 1610554066650.0)),
     ('track2', (54.80605608180558, 89.56267104710837, 1610554066656.0)),
     ('track2', (54.80562499100762, 89.56276241803405, 1610554066667.0)),
     ('track2', (54.80530592291032, 89.56270752401652, 1610554066675.0)),
     ('track2', (54.80550311865661, 89.56272360998025, 1610554066680.0)),
     ('track2', (54.8057106522356, 89.56307104001947, 1610554066690.0)),
     ('track2', (54.80577489553887, 89.56325817023767, 1610554066695.0)),
     ('track2', (54.805820225767654, 89.56293760359213, 1610554066703.0)),
     ('track2', (54.80580444075323, 89.56321594605913, 1610554066710.0)),
     ('track2', (54.80551188369847, 89.56335461174915, 1610554066718.0)),
     ('track2', (54.805925254077756, 89.56350729721564, 1610554066729.0)),
     ('track2', (54.80581532667179, 89.56329078860126, 1610554066735.0)),
     ('track2', (54.80543715627635, 89.56314668804414, 1610554066745.0)),
     ('track2', (54.805827182316605, 89.56335179194481, 1610554066756.0)),
     ('track2', (54.8056655984694, 89.56323761847632, 1610554066761.0)),
     ('track2', (54.80584635236756, 89.56296901871427, 1610554066769.0)),
     ('track2', (54.805566849008095, 89.56257188154557, 1610554066781.0)),
     ('track2', (54.80546228508102, 89.56273984376077, 1610554066786.0)),
     ('track2', (54.805652655261774, 89.56253617027231, 1610554066793.0)),
     ('track2', (54.80589539531696, 89.56254227603094, 1610554066799.0)),
     ('track2', (54.80579335313992, 89.56237276994052, 1610554066804.0)),
     ('track2', (54.80571030143242, 89.56210663818838, 1610554066811.0)),
     ('track2', (54.80558060093134, 89.56190136326371, 1610554066817.0)),
     ('track2', (54.805894956388414, 89.56207624992058, 1610554066826.0)),
     ('track2', (54.80576744089163, 89.56228288925071, 1610554066832.0)),
     ('track2', (54.80536153661632, 89.56201627758931, 1610554066844.0)),
     ('track2', (54.80508351407019, 89.56178800547103, 1610554066853.0)),
     ('track2', (54.80541519239853, 89.56214273007579, 1610554066865.0)),
     ('track2', (54.80511589126832, 89.56252516816512, 1610554066877.0)),
     ('track2', (54.8053709085876, 89.56263781745491, 1610554066884.0)),
     ('track2', (54.80544283369495, 89.56286973725487, 1610554066890.0)),
     ('track2', (54.80550997167529, 89.56322314521705, 1610554066899.0)),
     ('track2', (54.80547940985504, 89.56362668430276, 1610554066909.0)),
     ('track2', (54.8057445588973, 89.56332094944388, 1610554066919.0)),
     ('track2', (54.80593667211626, 89.56327364794619, 1610554066924.0)),
     ('track2', (54.80576859935785, 89.56281802572703, 1610554066936.0)),
     ('track2', (54.805621681676655, 89.56262469903557, 1610554066942.0)),
     ('track2', (54.80555501000072, 89.56247718856612, 1610554066946.0)),
     ('track2', (54.80581579760179, 89.56222940938365, 1610554066955.0)),
     ('track2', (54.80600111270646, 89.5626292171301, 1610554066966.0)),
     ('track2', (54.80623361330438, 89.56290371333792, 1610554066975.0)),
     ('track2', (54.80587401823699, 89.56291351556486, 1610554066984.0)),
     ('track2', (54.80619749634635, 89.56290010972138, 1610554066992.0)),
     ('track2', (54.80637026281386, 89.56280369048945, 1610554066997.0)),
     ('track2', (54.80667799410101, 89.56261739461979, 1610554067006.0)),
     ('track2', (54.8064325750618, 89.56282855166813, 1610554067014.0)),
     ('track2', (54.80675315830843, 89.56278333899455, 1610554067022.0)),
     ('track2', (54.80684054602969, 89.56260583320096, 1610554067027.0)),
     ('track2', (54.80668162450978, 89.56283489137289, 1610554067034.0)),
     ('track2', (54.80652292645657, 89.56280296365958, 1610554067038.0)),
     ('track2', (54.80663486840232, 89.56254763505483, 1610554067045.0)),
     ('track2', (54.806322183070925, 89.56263157364786, 1610554067053.0)),
     ('track2', (54.80604138371998, 89.56285642119765, 1610554067062.0)),
     ('track2', (54.80606456975054, 89.56265993372078, 1610554067067.0)),
     ('track2', (54.80567888486111, 89.5623648282739, 1610554067079.0)),
     ('track2', (54.80585359952853, 89.56191171146368, 1610554067091.0)),
     ('track2', (54.80553006401333, 89.56192365191079, 1610554067099.0)),
     ('track2', (54.805379657521556, 89.56221034981994, 1610554067107.0)),
     ('track2', (54.805240781707276, 89.56250280718791, 1610554067115.0)),
     ('track2', (54.80545649434265, 89.56226138253545, 1610554067123.0)),
     ('track2', (54.80571007276942, 89.56206009720229, 1610554067131.0)),
     ('track2', (54.80530560433693, 89.56204656554796, 1610554067141.0)),
     ('track2', (54.80550734885757, 89.5623973888182, 1610554067151.0)),
     ('track2', (54.8053274060668, 89.56247964155452, 1610554067156.0)),
     ('track2', (54.80508878986767, 89.56269845680394, 1610554067164.0)),
     ('track2', (54.80535150146685, 89.56260514918758, 1610554067171.0)),
     ('track2', (54.80531456999806, 89.56279952250672, 1610554067176.0)),
     ('track2', (54.80566227525431, 89.56307024187603, 1610554067187.0)),
     ('track2', (54.80549391610691, 89.56266300346965, 1610554067198.0)),
     ('track2', (54.80515089291352, 89.56277135375777, 1610554067207.0)),
     ('track2', (54.80499650296828, 89.56289508194935, 1610554067212.0)),
     ('track2', (54.8046118699046, 89.56276924385944, 1610554067222.0)),
     ('track2', (54.80453419048086, 89.56308354262018, 1610554067230.0)),
     ('track2', (54.80444038641997, 89.56321547157503, 1610554067234.0)),
     ('track2', (54.80455105482969, 89.56337947591544, 1610554067239.0)),
     ('track2', (54.804408331482925, 89.5631830323842, 1610554067245.0)),
     ('track2', (54.80446705665974, 89.56274629530569, 1610554067256.0)),
     ('track2', (54.80420304217928, 89.56309911859893, 1610554067267.0)),
     ('track2', (54.804107272914514, 89.56296860921653, 1610554067271.0)),
     ('track2', (54.803646210479734, 89.56281609203734, 1610554067283.0)),
     ('track2', (54.80368721111635, 89.56254033373118, 1610554067290.0)),
     ('track2', (54.80380709217443, 89.56211628602861, 1610554067301.0)),
     ('track2', (54.80371246826473, 89.5619425297732, 1610554067306.0)),
     ('track2', (54.803353101439505, 89.5619263996439, 1610554067315.0)),
     ('track2', (54.803233343990804, 89.56217815716434, 1610554067322.0)),
     ('track2', (54.80290008513939, 89.56188984056735, 1610554067333.0)),
     ('track2', (54.80304549321268, 89.56235319415904, 1610554067345.0)),
     ('track2', (54.80306960673395, 89.56219312233435, 1610554067349.0)),
     ('track2', (54.802836414307144, 89.56246703105677, 1610554067358.0)),
     ('track2', (54.8025378111302, 89.56259214678956, 1610554067366.0)),
     ('track2', (54.80280733456972, 89.56289403242039, 1610554067376.0)),
     ('track2', (54.80262665216265, 89.56297464748324, 1610554067381.0)),
     ('track2', (54.802395339948845, 89.56274812493774, 1610554067389.0)),
     ('track2', (54.80231661891254, 89.56288957262917, 1610554067393.0)),
     ('track2', (54.80213089355394, 89.56309749036961, 1610554067400.0)),
     ('track2', (54.80218738958916, 89.56353452139844, 1610554067411.0))]

  def test_pipeline_unbounded(self):
    with beam.Pipeline(options=PipelineOptions(["--streaming"])) as p:
        res = (p
            | TestStream()
               .add_elements(TestSportTracker.data)
               .advance_watermark_to_infinity()
            | SportTrackerCalc()
            | beam.Map(lambda x: "%s:%d,%d" % (x[0], round(x[2]), round(x[1]))))
        assert_that(res, equal_to(["track1:614,257", "track2:5641,1262"]))

if __name__ == '__main__':
    unittest.main()