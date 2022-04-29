SELECT *
FROM
(
  VALUES
    (CHAR 'AAAAAAAAAAAAAABA', CHAR 'Scott               ', CHAR 'Gordon                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAAEBA', CHAR 'Donna               ', CHAR 'Barnes                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAAGCA', CHAR 'Antonio             ', CHAR 'Carney                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAAJCA', CHAR 'Marian              ', CHAR 'Sweet                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAALDA', CHAR 'Joseph              ', CHAR 'Hart                          ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAANDA', CHAR 'Alexandria          ', CHAR 'Lancaster                     ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABBDA', CHAR 'Stanley             ', CHAR 'Oliver                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAABCDA', CHAR 'Matthew             ', CHAR 'Simon                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABDBA', CHAR 'Magdalena           ', CHAR 'Fitts                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABEBA', CHAR 'Hermila             ', CHAR 'Hale                          ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAABFDA', CHAR 'Donald              ', CHAR 'Thompson                      ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABGCA', CHAR 'Larry               ', CHAR 'Butcher                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAABGDA', CHAR 'Mildred             ', CHAR 'Ruiz                          ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABHAA', CHAR 'Irene               ', CHAR 'Cromwell                      ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABHCA', CHAR 'Kimberly            ', CHAR 'Rios                          ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABICA', CHAR 'Emily               ', CHAR 'Sharp                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABJBA', CHAR 'Christina           ', CHAR 'Landry                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAABJCA', CHAR 'Rosa                ', CHAR 'Brennan                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABJDA', CHAR 'Wayne               ', CHAR 'Wilson                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABLAA', CHAR 'Pauline             ', CHAR 'Green                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABMCA', CHAR 'Peter               ', CHAR 'Townsend                      ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABNAA', CHAR 'Derek               ', CHAR 'Zuniga                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAABNCA', CHAR 'Micheal             ', CHAR 'Davis                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAABNDA', CHAR 'Samuel              ', CHAR 'Taylor                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAACAAA', CHAR 'Francis             ', CHAR 'Dow                           ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAACADA', CHAR 'Brian               ', CHAR 'Hicks                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAACCBA', CHAR 'William             ', CHAR 'Shively                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAACCCA', CHAR 'Dorothy             ', CHAR 'Walker                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAACEDA', CHAR 'Mike                ', CHAR 'Mcclung                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAACHDA', CHAR 'Jim                 ', CHAR 'Alford                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAACJAA', CHAR 'Jason               ', CHAR 'Calloway                      ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAACKBA', CHAR 'Cheryl              ', CHAR 'Jones                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAACLBA', NULL, NULL, CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAACMCA', CHAR 'Evalyn              ', CHAR 'Walker                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAACNAA', CHAR 'Andrea              ', CHAR 'Palmer                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAACNBA', CHAR 'Douglas             ', CHAR 'Bradley                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAACNCA', CHAR 'Georgie             ', CHAR 'Rice                          ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAADCDA', CHAR 'Florida             ', CHAR 'Corey                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAADDAA', NULL, NULL, NULL),
    (CHAR 'AAAAAAAAAAAADFAA', CHAR 'Julia               ', CHAR 'Mathews                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAADHCA', CHAR 'Anna                ', CHAR 'Pelletier                     ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAADIAA', NULL, NULL, NULL),
    (CHAR 'AAAAAAAAAAAADLAA', CHAR 'Henry               ', CHAR 'Carroll                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAADMBA', CHAR 'Melissa             ', CHAR 'Crews                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAADMDA', CHAR 'Margie              ', NULL, CHAR 'N'),
    (CHAR 'AAAAAAAAAAAADNAA', CHAR 'John                ', CHAR 'Miller                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAADPBA', CHAR 'Carolann            ', CHAR 'Bean                          ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAADPCA', CHAR 'Gerald              ', NULL, CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAECAA', CHAR 'Harold              ', CHAR 'Stewart                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAEDCA', CHAR 'Erwin               ', CHAR 'Sherman                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAEEBA', CHAR 'John                ', CHAR 'Faulkner                      ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAEFCA', CHAR 'Ignacio             ', CHAR 'Bishop                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAEGBA', CHAR 'Joey                ', CHAR 'Shockley                      ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAEHAA', CHAR 'Lee                 ', CHAR 'Jewell                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAEHDA', CHAR 'Gerald              ', CHAR 'Blair                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAEIAA', CHAR 'Jonathan            ', CHAR 'Jones                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAEJBA', CHAR 'Robert              ', CHAR 'Hawkins                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAEJDA', CHAR 'Peggy               ', CHAR 'Gordon                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAEKDA', CHAR 'Tom                 ', CHAR 'Davis                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAELCA', CHAR 'Latoya              ', CHAR 'Harris                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAEMDA', CHAR 'Scott               ', CHAR 'Cole                          ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAENDA', CHAR 'Daniel              ', CHAR 'Gonzales                      ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAEPAA', CHAR 'Regina              ', CHAR 'Molina                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFADA', CHAR 'Steven              ', CHAR 'Strickland                    ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAFBBA', CHAR 'Freddy              ', CHAR 'Hansen                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFBCA', CHAR 'Joseph              ', CHAR 'Waters                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFBDA', CHAR 'Jocelyn             ', CHAR 'Cain                          ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFDAA', CHAR 'Edward              ', CHAR 'Clifton                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFDBA', CHAR 'Jesus               ', CHAR 'Baker                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAFFAA', CHAR 'Alma                ', CHAR 'Blankenship                   ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAFFCA', CHAR 'Kenneth             ', CHAR 'Flynn                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAFFDA', CHAR 'Sanford             ', CHAR 'Johnson                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFGAA', NULL, CHAR 'Andersen                      ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFGBA', CHAR 'Dirk                ', CHAR 'Davis                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFHBA', CHAR 'Esther              ', CHAR 'Anthony                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFHCA', CHAR 'Nancy               ', CHAR 'Grimes                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAFIAA', CHAR 'Kay                 ', CHAR 'Mccarthy                      ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAFIBA', CHAR 'Carl                ', CHAR 'Patterson                     ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFKDA', CHAR 'James               ', CHAR 'Elder                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFLBA', CHAR 'Naomi               ', CHAR 'Burnette                      ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAFMBA', CHAR 'Thomas              ', CHAR 'Adams                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAFMCA', CHAR 'Judy                ', CHAR 'Price                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAFOCA', CHAR 'Catherine           ', CHAR 'Seibert                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAGACA', CHAR 'Conrad              ', CHAR 'Ridley                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAGBCA', CHAR 'Ashley              ', CHAR 'Broderick                     ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAGDAA', CHAR 'Carlos              ', CHAR 'Navarro                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAGFAA', CHAR 'Elizabeth           ', CHAR 'Mason                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAGHBA', CHAR 'Joy                 ', CHAR 'Lawhorn                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAGHCA', CHAR 'Sandra              ', CHAR 'Rogers                        ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAGIAA', CHAR 'Brett               ', CHAR 'Martinez                      ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAGIDA', CHAR 'Christopher         ', CHAR 'Ortiz                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAGLDA', CHAR 'Joshua              ', CHAR 'Aiken                         ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAGMCA', CHAR 'Fredric             ', CHAR 'Osborne                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAGNBA', CHAR 'Genevieve           ', CHAR 'Allen                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAGNDA', CHAR 'Theodore            ', CHAR 'Ervin                         ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAGOCA', CHAR 'Amanda              ', CHAR 'Asbury                        ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAHBAA', CHAR 'Shirlene            ', CHAR 'Wortham                       ', CHAR 'N'),
    (CHAR 'AAAAAAAAAAAAHDBA', NULL, CHAR 'Brock                         ', NULL),
    (CHAR 'AAAAAAAAAAAAHEDA', CHAR 'Michael             ', CHAR 'Cormier                       ', CHAR 'Y'),
    (CHAR 'AAAAAAAAAAAAHFCA', CHAR 'Maryetta            ', CHAR 'Ruiz                          ', CHAR 'Y')
)
