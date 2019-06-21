/*Raymond Lin
304937942
CS 143

Homework 3
Part 1:*/

SELECT 
	company,
	SUM(CASE value WHEN 'agile-dev' THEN 1 ELSE 0 END) AS fast_paced,
	SUM(CASE value WHEN 'benefit-company' THEN 1 ELSE 0 END) AS benefit_company,
	SUM(CASE value WHEN 'bonded-by-product' THEN 1 ELSE 0 END) AS bonded_by_product,
	SUM(CASE value WHEN 'continuous-delivery' THEN 1 ELSE 0 END) AS continuous_delivery,
	SUM(CASE value WHEN 'creative-innovative' THEN 1 ELSE 0 END) AS creative_innovative,
	SUM(CASE value WHEN 'cross-dep' THEN 1 ELSE 0 END) AS cross_dep,
	SUM(CASE value WHEN 'customer-first' THEN 1 ELSE 0 END) AS customer_first,
	SUM(CASE value WHEN 'data-driven' THEN 1 ELSE 0 END) AS data_driven,
	SUM(CASE value WHEN 'diverse-team' THEN 1 ELSE 0 END) AS diverse_team,
	SUM(CASE value WHEN 'engages-community' THEN 1 ELSE 0 END) AS engages_community,
	SUM(CASE value WHEN 'engineering-driven' THEN 1 ELSE 0 END) AS engineering_driven,
	SUM(CASE value WHEN 'eq-iq' THEN 1 ELSE 0 END) AS eq_iq,
	SUM(CASE value WHEN 'fast-paced' THEN 1 ELSE 0 END) AS fast_paced,
	SUM(CASE value WHEN 'feedback' THEN 1 ELSE 0 END) AS feedback,
	SUM(CASE value WHEN 'flat-organization' THEN 1 ELSE 0 END) AS flat_organization,
	SUM(CASE value WHEN 'flex-hours' THEN 1 ELSE 0 END) AS flex_hours,
	SUM(CASE value WHEN 'friends-outside-work' THEN 1 ELSE 0 END) AS friends_outside_work,
	SUM(CASE value WHEN 'good-beer' THEN 1 ELSE 0 END) AS good_beer,
	SUM(CASE value WHEN 'impressive-teammates' THEN 1 ELSE 0 END) AS impressive_teammates,
	SUM(CASE value WHEN 'inclusive' THEN 1 ELSE 0 END) AS inclusive,
	SUM(CASE value WHEN 'internal-mobility' THEN 1 ELSE 0 END) AS internal_mobility,
	SUM(CASE value WHEN 'internal-promotion' THEN 1 ELSE 0 END) AS internal_promotion,
	SUM(CASE value WHEN 'interns' THEN 1 ELSE 0 END) AS interns,
	SUM(CASE value WHEN 'junior-devs' THEN 1 ELSE 0 END) AS junior_devs,
	SUM(CASE value WHEN 'light-meetings' THEN 1 ELSE 0 END) AS light_meetings,
	SUM(CASE value WHEN 'lunch-together' THEN 1 ELSE 0 END) AS lunch_together,
	SUM(CASE value WHEN 'many-hats' THEN 1 ELSE 0 END) AS many_hats,
	SUM(CASE value WHEN 'new-tech' THEN 1 ELSE 0 END) AS new_tech,
	SUM(CASE value WHEN 'office-layout' THEN 1 ELSE 0 END) AS office_layout,
	SUM(CASE value WHEN 'open-communication' THEN 1 ELSE 0 END) AS open_communication,
	SUM(CASE value WHEN 'open-source' THEN 1 ELSE 0 END) AS open_source,
	SUM(CASE value WHEN 'pair-programs' THEN 1 ELSE 0 END) AS pair_programs,
	SUM(CASE value WHEN 'parents' THEN 1 ELSE 0 END) AS ideal_for_parents,
	SUM(CASE value WHEN 'personal-growth' THEN 1 ELSE 0 END) AS personal_growth,
	SUM(CASE value WHEN 'physical-wellness' THEN 1 ELSE 0 END) AS physical_wellness,
	SUM(CASE value WHEN 'product-driven' THEN 1 ELSE 0 END) AS product_driven,
	SUM(CASE value WHEN 'project-ownership' THEN 1 ELSE 0 END) AS project_ownership,
	SUM(CASE value WHEN 'psychologically-safe' THEN 1 ELSE 0 END) AS psychologically_safe,
	SUM(CASE value WHEN 'quality-code' THEN 1 ELSE 0 END) AS quality_code,
	SUM(CASE value WHEN 'rapid-growth' THEN 1 ELSE 0 END) AS rapid_growth,
	SUM(CASE value WHEN 'remote-ok' THEN 1 ELSE 0 END) AS remote_ok,
	SUM(CASE value WHEN 'retention' THEN 1 ELSE 0 END) AS retention,
	SUM(CASE value WHEN 'risk-taking' THEN 1 ELSE 0 END) AS risk_taking,
	SUM(CASE value WHEN 'safe-env' THEN 1 ELSE 0 END) AS safe_env,
	SUM(CASE value WHEN 'team-oriented' THEN 1 ELSE 0 END) AS team_oriented,
	SUM(CASE value WHEN 'worklife-balance' THEN 1 ELSE 0 END) AS worklife_balance
FROM hw3.keyvalues
GROUP BY company;
