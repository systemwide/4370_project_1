
/*****************************************************************************************
 * @file  MovieDB.java
 *
 * @author   John Miller
 */

import static java.lang.System.out;

/*****************************************************************************************
 * The MovieDB class makes a Movie Database.  It serves as a template for making other
 * databases.  See "Database Systems: The Complete Book", second edition, page 26 for more
 * information on the Movie Database schema.
 */
class MovieDB
{
    /*************************************************************************************
     * Main method for creating, populating and querying a Movie Database.
     * @param args  the command-line arguments
     */
    public static void main (String [] args)
    {
        out.println ();

        Table movie = new Table ("movie", "title year length genre studioName producerNo",
                                          "String Integer Integer String String Integer", "title year");

        Table cinema = new Table ("cinema", "title year length genre studioName producerNo",
                                            "String Integer Integer String String Integer", "title year");

        Table movieStar = new Table ("movieStar", "name address gender birthdate",
                                                  "String String Character String", "name");

        Table starsIn = new Table ("starsIn", "movieTitle movieYear starName",
                                              "String Integer String", "movieTitle movieYear starName");

        Table movieExec = new Table ("movieExec", "certNo name address fee",
                                                  "Integer String String Float", "certNo");

        Table studio = new Table ("studio", "name address presNo",
                                            "String String Integer", "name");

        Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        Comparable [] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
        Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
        Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
        out.println ();
        movie.insert (film0);
        movie.insert (film1);
        movie.insert (film2);
        movie.insert (film3);
        movie.print ();

        Comparable [] film4 = { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
        out.println ();
        cinema.insert (film2);
        cinema.insert (film3);
        cinema.insert (film4);
        cinema.print ();

        Comparable [] star0 = { "Carrie_Fisher", "Hollywood", 'F', "9/9/99" };
        Comparable [] star1 = { "Mark_Hamill", "Brentwood", 'M', "8/8/88" };
        Comparable [] star2 = { "Harrison_Ford", "Beverly_Hills", 'M', "7/7/77" };
        out.println ();
        movieStar.insert (star0);
        movieStar.insert (star1);
        movieStar.insert (star2);
        movieStar.print ();

        Comparable [] cast0 = { "Star_Wars", 1977, "Carrie_Fisher" };
        out.println ();
        starsIn.insert (cast0);
        starsIn.print ();

        Comparable [] exec0 = { "a9999", "S_Spielberg", "Hollywood", 10000 };
        out.println ();
        movieExec.insert (exec0);
        movieExec.print ();

        Comparable [] studio0 = { "Fox", "Los_Angeles", 7777 };
        Comparable [] studio1 = { "Universal", "Universal_City", 8888 };
        Comparable [] studio2 = { "DreamWorks", "Universal_City", 9999 };
        out.println ();
        studio.insert (studio0);
        studio.insert (studio1);
        studio.insert (studio2);
        studio.print ();

        movie.save ();
        cinema.save ();
        movieStar.save ();
        starsIn.save ();
        movieExec.save ();
        studio.save ();

        movieStar.printIndex ();

        //--------------------- project: title year

        out.println ();
        Table t_project = movie.project ("title year");
        t_project.print ();

        //--------------------- project: non-existent arguments
        /**
         *@author Ben Rotolo
         *
         */
        out.println ("Testing made up terms");
        Table t_project2 = movie.project ("made up terms");
        t_project2.print ();
        
        //--------------------- project: no arguments
        /**
         *@author Ben Rotolo
         *
         */
        out.println ("Testing no terms");
        Table t_project3 = movie.project ("");
        t_project3.print ();


        //--------------------- select: equals, &&

        out.println ();
        Table t_select = movie.select (t -> t[movie.col("title")].equals ("Star_Wars") &&
                                            t[movie.col("year")].equals (1977));
        t_select.print ();
        
        
        //--------------------- select: equals, ||
        /**
         *@author Ben Rotolo
         *
         */
        out.println ("Testing Select with .equals and || ");
        out.println ();
        Table t_select_or = movie.select (t -> t[movie.col("title")].equals ("Star_Wars") ||
                                             t[movie.col("title")].equals ("Star_Wars_2")) ;
        t_select_or.print ();
        
        //--------------------- select: !equals
        /**
         *@author Ben Rotolo
         *
         */
        out.println ("Testing Select with !equals");
        out.println ();
        Table t_select_not = movie.select (t -> !t[movie.col("title")].equals ("Star_Wars"));
                                            
        t_select_not.print ();
        
        //--------------------- select: <

        out.println ();
        Table t_select2 = movie.select (t -> (Integer) t[movie.col("year")] < 1980);
        t_select2.print ();

        //--------------------- indexed select: key

        out.println ();
        Table t_iselect = movieStar.select (new KeyType ("Harrison_Ford"));
        t_iselect.print ();
        
        //--------------------- select with blank key
        /**
         *@author Ben Rotolo
         *
         */
        out.println ("Select with a  ' ' for key...");
        Table t_iselect_blank = movieStar.select (new KeyType (" "));
        t_iselect_blank.print ();
        
        //--------------------- select without args
        /**
         *@author Ben Rotolo
         *
         */
        out.println ("Select with out arguments...");
        Table t_iselect_no_arg = movieStar.select ();
        t_iselect_no_arg.print ();

     
        //--------------------- union: movie UNION cinema

        out.println ();
        Table t_union = movie.union (cinema);
        t_union.print ();
        
        //--------------------- union: movie UNION cinema
        /**
         *@author Ben Rotolo
         *
         */
        out.println("self-Union movie with movie");
        Table t_union_self = movie.union(movie);
        t_union_self.print();

        
        //--------------------- union: movie UNION movie
        /**
         *@author Ben Rotolo
         *
         */
        out.println("self-Union movie with movie");
        Table t_union_self1 = movie.union(movie);
        t_union_self1.print();

        //--------------------- minus: movie MINUS cinema

        out.println ();
        Table t_minus = movie.minus (cinema);
        t_minus.print ();
        
        //--------------------- minus: movie MINUS movie
        /**
         *@author Ben Rotolo
         *
         */
        out.println ("Movie minus self...");
        Table self_minus = movie.minus (movie);
        self_minus.print ();

        
        //--------------------- equi-join: movie JOIN studio ON studioName = name

        out.println ();
        Table t_join = movie.join ("studioName", "name", studio);
        t_join.print ();

        
        //--------------------- natural join: movie JOIN studio

        out.println ();
        Table t_join2 = movie.join (cinema);
        t_join2.print ();
        
        //--------------------- natural join: join on self.
        /**
         *@author Ben Rotolo
         *
         */
        out.println ("Movie join with movie");
        Table self_join = movie.join (movie);
        self_join.print ();

    } // main

} // MovieDB class
